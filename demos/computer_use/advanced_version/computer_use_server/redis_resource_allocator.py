# -*- coding: utf-8 -*-
"""
Redis-based Resource Allocator for Multi-Instance Deployment
基于Redis的集群级资源分配器
"""
import time
import asyncio
import uuid
import os
from typing import Tuple, List, Dict, Any, Optional
import redis.asyncio as redis
from agentscope_bricks.utils.logger_util import logger
from enum import Enum, auto
from agentscope_runtime.sandbox.box.cloud_api.cloud_phone_sandbox import (
    CloudPhoneSandbox,
)
from agentscope_runtime.sandbox.box.cloud_api.cloud_computer_sandbox import (
    CloudComputerSandbox,
)
from fastapi import HTTPException


class AllocationStatus(Enum):
    """资源分配操作结果状态码"""

    SUCCESS = auto()  # 分配/释放成功
    RESOURCE_EXHAUSTED = auto()  # 资源已耗尽
    ALREADY_ALLOCATED = auto()  # 实例已被分配
    NOT_ALLOCATED = auto()  # 实例未被分配
    INVALID_INSTANCE = auto()  # 无效实例ID
    WAIT_TIMEOUT = auto()  # 等待超时
    CHAT_ALREADY_ALLOCATED = auto()  # 对话已有分配实例


class AsyncRedisResourceAllocator:
    """基于Redis的异步资源分配器"""

    def __init__(
        self,
        resource_type: str,
        instance_ids: List[str],
        redis_client: redis.Redis,
    ):
        """
        初始化Redis资源分配器

        Args:
            resource_type: 资源类型 ("phone" 或 "pc")
            instance_ids: 实例ID列表
            redis_client: Redis客户端
        """
        self.resource_type = resource_type
        self.instance_ids = set(instance_ids)
        self.redis = redis_client
        self.total_instances = len(instance_ids)

        # Redis key前缀
        self.prefix = f"resource_allocator:{resource_type}"
        self.FREE_INSTANCES_KEY = f"{self.prefix}:free_instances"
        self.ALLOCATIONS_KEY = (
            f"{self.prefix}:allocations"  # {instance_id: user_id}
        )
        self.USER_ALLOCATIONS_KEY = (
            f"{self.prefix}:user_allocations"  # {user_id: instance_id}
        )
        self.QUEUE_KEY = f"{self.prefix}:queue"  # 排队列表
        self.QUEUE_TIMESTAMPS_KEY = (
            f"{self.prefix}:queue_timestamps"  # {user_id: timestamp}
        )

        # 分布式锁
        self.LOCK_KEY = f"{self.prefix}:lock"
        self.LOCK_TIMEOUT = 10  # 锁超时时间

        # TTL配置
        self.ALLOCATION_TTL = 7200  # 分配记录2小时过期
        self.QUEUE_TTL = 3600  # 排队记录1小时过期

    async def initialize(self):
        """初始化资源池"""
        async with self._distributed_lock("init"):
            # 检查是否已初始化
            exists = await self.redis.exists(self.FREE_INSTANCES_KEY)
            if not exists:
                # 初始化所有实例为可用状态
                if self.instance_ids:
                    await self.redis.sadd(
                        self.FREE_INSTANCES_KEY,
                        *self.instance_ids,
                    )
                    logger.info(
                        f"Initialized {self.resource_type} resource pool with "
                        f"{len(self.instance_ids)} instances",
                    )

    async def allocate_async(
        self,
        user_id: str,
        timeout: Optional[float] = None,
    ) -> Tuple[str, AllocationStatus]:
        """
        异步分配资源

        Args:
            user_id: 用户ID
            timeout: 超时时间（None为无限等待，0为非阻塞）

        Returns:
            (instance_id, status) 元组
        """
        logger.info(f"[{self.resource_type}] 尝试为用户 {user_id} 分配资源")

        async with self._distributed_lock(f"allocate:{user_id}"):
            # 检查用户是否已有分配
            existing_instance = await self.redis.hget(
                self.USER_ALLOCATIONS_KEY,
                user_id,
            )
            if existing_instance:
                if isinstance(existing_instance, bytes):
                    existing_instance = existing_instance.decode("utf-8")
                logger.info(
                    f"[{self.resource_type}] 用户 {user_id} 已有分配: "
                    f"{existing_instance}",
                )
                return (
                    existing_instance,
                    AllocationStatus.CHAT_ALREADY_ALLOCATED,
                )

            # 尝试分配可用资源
            instance_id = await self.redis.spop(self.FREE_INSTANCES_KEY)
            if instance_id:
                if isinstance(instance_id, bytes):
                    instance_id = instance_id.decode("utf-8")

                # 记录分配
                await self._record_allocation(user_id, instance_id)
                logger.info(
                    f"[{self.resource_type}] 成功为用户 {user_id} 分配资源: "
                    f"{instance_id}",
                )
                return instance_id, AllocationStatus.SUCCESS

            # 无可用资源
            if self.total_instances == 0:
                return "", AllocationStatus.RESOURCE_EXHAUSTED

            # 处理排队
            if timeout == 0:
                # 非阻塞模式：加入排队但立即返回
                await self._add_to_queue(user_id)
                logger.info(
                    f"[{self.resource_type}] 用户 {user_id} 加入排队队列",
                )
                return "", AllocationStatus.WAIT_TIMEOUT

            # 阻塞模式：等待资源释放
            return await self._wait_for_resource(user_id, timeout)

    async def release_async(self, instance_id: str) -> AllocationStatus:
        """
        异步释放资源

        Args:
            instance_id: 要释放的实例ID

        Returns:
            操作状态码
        """
        logger.info(f"[{self.resource_type}] 尝试释放资源: {instance_id}")

        async with self._distributed_lock(f"release:{instance_id}"):
            # 验证实例ID
            if instance_id not in self.instance_ids:
                return AllocationStatus.INVALID_INSTANCE

            # 检查是否已分配
            user_id = await self.redis.hget(self.ALLOCATIONS_KEY, instance_id)
            if not user_id:
                logger.warning(
                    f"[{self.resource_type}] 实例 {instance_id} 未被分配",
                )
                return AllocationStatus.NOT_ALLOCATED

            if isinstance(user_id, bytes):
                user_id = user_id.decode("utf-8")

            # 清理分配记录
            await self._clear_allocation(user_id, instance_id)
            # 休眠,重置设备
            if self.resource_type == "phone":
                logger.info(
                    f"[{self.resource_type}] 休眠,重置设备: {instance_id}",
                )
                equipment = await asyncio.to_thread(
                    CloudPhoneSandbox,
                    instance_id=instance_id,
                )
                e_client = equipment.instance_manager.eds_client
                should_reset_image = os.environ.get("EQUIP_RESET", 1) == "1"
                if should_reset_image:
                    await self._wait_for_phone_ready(equipment, instance_id)
                    # 重置实例镜像
                    print(f"Equipment reset for user {user_id}")
                    logger.info(f"Equipment reset for user {user_id}")
                    method = e_client.reset_equipment
                    status = await method([instance_id])
                    if status != 200:
                        raise HTTPException(
                            503,
                            "Failed to reset phone resource",
                        )
                else:
                    logger.info(
                        "跳过手机镜像重置: 同一会话重新激活或EQUIP_RESET未启用",
                    )
                await self._wait_for_phone_ready(
                    equipment,
                    instance_id,
                    stability_check_duration=10,
                )
                # 停止设备
                method = e_client.stop_equipment_async
                status = await method(
                    [instance_id],
                )
                if status != 200:
                    raise HTTPException(
                        503,
                        "Failed to stop equipment resource",
                    )
            elif self.resource_type == "pc":
                equipment = await asyncio.to_thread(
                    CloudComputerSandbox,
                    desktop_id=instance_id,
                )
                should_reset_image = os.environ.get("EQUIP_RESET", 1) == "1"
                e_client = equipment.instance_manager.ecd_client
                if should_reset_image:
                    logger.info("查询设备状态")
                    await self._wait_for_pc_ready(
                        equipment,
                        instance_id,
                        stability_check_duration=2,
                    )
                    # 重置实例镜像
                    print(f"Equipment reset for user {user_id}")
                    logger.info(f"Equipment reset for user {user_id}")
                    method = e_client.rebuild_equipment_image
                    status = await method(
                        instance_id,
                        os.environ.get("ECD_IMAGE_ID"),
                    )
                    if status != 200:
                        raise HTTPException(
                            503,
                            "Failed to reset computer resource",
                        )
                else:
                    logger.info(
                        "跳过镜像重置: 同一会话重新激活或EQUIP_RESET未启用 ",
                    )
                # 休眠设备
                await self._wait_for_pc_ready(
                    equipment,
                    instance_id,
                    stability_check_duration=2,
                )
                method = e_client.hibernate_desktops_async
                status = await method(
                    [instance_id],
                )
                if status != 200:
                    raise HTTPException(
                        503,
                        "Failed to hibernate equipment resource",
                    )
            # 将实例放回可用池
            await self.redis.sadd(self.FREE_INSTANCES_KEY, instance_id)

            logger.info(f"[{self.resource_type}] 资源 {instance_id} 已释放")

            # 通知排队用户
            await self._notify_queued_users()

            return AllocationStatus.SUCCESS

    async def _wait_for_phone_ready(
        self,
        equipment,
        instance_id: str,
        max_wait_time: int = 300,
        stability_check_duration: int = 4,
    ):
        """异步等待手机设备就绪"""
        start_time = time.time()
        stable_start_time = None
        while True:
            try:
                # 将同步的状态检查操作放到线程池中执行
                total_count, next_token, devices_info = (
                    await asyncio.to_thread(
                        equipment.instance_manager.eds_client.list_instance,
                        instance_ids=[instance_id],
                    )
                )

                if (
                    devices_info
                    and devices_info[0].android_instance_status.lower()
                    == "running"
                ):
                    # 第一次检测到运行状态，开始稳定性检查
                    if stable_start_time is None:
                        stable_start_time = time.time()
                        print(
                            f"Phone {instance_id} status: running, "
                            "starting stability check...",
                        )

                    # 检查设备是否已稳定运行足够长时间
                    stable_duration = time.time() - stable_start_time
                    if stable_duration >= stability_check_duration:
                        print(
                            f"✓ Phone {instance_id} is stable and ready"
                            f" (stable for {stable_duration:.1f}s)",
                        )
                        break
                    else:
                        print(
                            f"Phone {instance_id} stability check: "
                            f"{stable_duration:.1f}"
                            f"s/{stability_check_duration}s",
                        )
                else:
                    # 状态不是运行中，重置稳定性检查
                    if stable_start_time is not None:
                        print(
                            f"PHONE {instance_id} status changed, "
                            "resetting stability check",
                        )
                        stable_start_time = None
                    current_status = (
                        devices_info[0].android_instance_status.lower()
                        if devices_info
                        else "unknown"
                    )
                    print(
                        f"PHONE {instance_id} status: "
                        f"{current_status}, waiting...",
                    )
                    if (
                        current_status == "stopped"
                        or current_status == "unknown"
                    ):
                        # 开机
                        print(
                            f"Equipment restart for instance_id {instance_id}",
                        )
                        logger.info(
                            f"Equipment restart for instance_id {instance_id}",
                        )
                        e_client = equipment.instance_manager.eds_client
                        method = e_client.start_equipment
                        status = await method(
                            [instance_id],
                        )
                        if status != 200:
                            raise HTTPException(
                                503,
                                "Failed to start computer resource",
                            )

                # 检查是否超时
                if time.time() - start_time > max_wait_time:
                    raise TimeoutError(
                        f"Phone {instance_id} failed to become ready "
                        f"within {max_wait_time} seconds",
                    )

            except Exception as e:
                print(f"Error checking phone status for {instance_id}: {e}")

            await asyncio.sleep(5)

    async def _wait_for_pc_ready(
        self,
        equipment,
        desktop_id: str,
        max_wait_time: int = 300,
        stability_check_duration: int = 10,
    ):
        """异步等待PC设备就绪，增加稳定性检查"""
        start_time = time.time()
        stable_start_time = None

        while True:
            try:
                # 将同步的状态检查操作放到线程池中执行
                pc_info = await asyncio.to_thread(
                    equipment.instance_manager.ecd_client.search_desktop_info,
                    [desktop_id],
                )

                if pc_info and pc_info[0].desktop_status.lower() == "running":
                    # 第一次检测到运行状态，开始稳定性检查
                    if stable_start_time is None:
                        stable_start_time = time.time()
                        print(
                            f"PC {desktop_id} status: running, "
                            "starting stability check...",
                        )

                    # 检查设备是否已稳定运行足够长时间
                    stable_duration = time.time() - stable_start_time
                    if stable_duration >= stability_check_duration:
                        print(
                            f"✓ PC {desktop_id} is stable and ready"
                            f" (stable for {stable_duration:.1f}s)",
                        )
                        break
                    else:
                        print(
                            f"PC {desktop_id} stability check: "
                            f"{stable_duration:.1f}"
                            f"s/{stability_check_duration}s",
                        )
                else:
                    # 状态不是运行中，重置稳定性检查
                    if stable_start_time is not None:
                        print(
                            f"PC {desktop_id} status changed, "
                            "resetting stability check",
                        )
                        stable_start_time = None
                    current_status = (
                        pc_info[0].desktop_status.lower()
                        if pc_info
                        else "unknown"
                    )
                    print(
                        f"PC {desktop_id} status: "
                        f"{current_status}, waiting...",
                    )
                    if (
                        current_status == "stopped"
                        or current_status == "unknown"
                    ):
                        # 开机
                        print(f"Equipment restart for desktop_id {desktop_id}")
                        logger.info(
                            f"Equipment restart for desktop_id {desktop_id}",
                        )
                        e_client = equipment.instance_manager.ecd_client
                        method = e_client.start_desktops_async
                        status = await method(
                            [desktop_id],
                        )
                        if status != 200:
                            raise HTTPException(
                                503,
                                "Failed to start computer resource",
                            )
                    elif current_status == "hibernated":
                        # 唤醒
                        print(f"Equipment wakeup for desktop_id {desktop_id}")
                        logger.info(
                            f"Equipment wakeup for desktop_id {desktop_id}",
                        )
                        e_client = equipment.instance_manager.ecd_client
                        method = e_client.wakeup_desktops_async
                        status = await method(
                            [desktop_id],
                        )
                        if status != 200:
                            raise HTTPException(
                                503,
                                "Failed to start computer resource",
                            )

                # 检查是否超时
                if time.time() - start_time > max_wait_time:
                    raise TimeoutError(
                        f"PC {desktop_id} failed to become ready"
                        f" within {max_wait_time} seconds",
                    )

            except Exception as e:
                print(f"Error checking PC status for {desktop_id}: {e}")
                # 出现异常时重置稳定性检查
                stable_start_time = None

            await asyncio.sleep(3)  # 减少检查间隔，更精确的监控

    async def get_chat_allocation_async(
        self,
        user_id: str,
    ) -> Tuple[str, AllocationStatus]:
        """获取用户分配的资源"""
        instance_id = await self.redis.hget(self.USER_ALLOCATIONS_KEY, user_id)
        if instance_id:
            if isinstance(instance_id, bytes):
                instance_id = instance_id.decode("utf-8")
            return instance_id, AllocationStatus.SUCCESS
        return "", AllocationStatus.NOT_ALLOCATED

    async def get_chat_position(
        self,
        user_id: str,
    ) -> Tuple[int, AllocationStatus]:
        """获取用户在排队中的位置"""
        # 使用LRANGE获取整个队列，然后查找位置
        queue_items = await self.redis.lrange(self.QUEUE_KEY, 0, -1)

        # 解码并查找用户位置
        for position, item in enumerate(queue_items):
            if isinstance(item, bytes):
                item = item.decode("utf-8")
            if item == user_id:
                return position, AllocationStatus.SUCCESS

        return -1, AllocationStatus.NOT_ALLOCATED

    async def cancel_wait_async(self, user_id: str) -> AllocationStatus:
        """取消用户排队"""
        async with self._distributed_lock(f"cancel:{user_id}"):
            # 从队列中移除
            removed = await self.redis.lrem(self.QUEUE_KEY, 1, user_id)
            await self.redis.hdel(self.QUEUE_TIMESTAMPS_KEY, user_id)

            if removed > 0:
                logger.info(
                    f"[{self.resource_type}] 用户 {user_id} 已取消排队",
                )
                return AllocationStatus.SUCCESS
            return AllocationStatus.NOT_ALLOCATED

    async def get_queue_info_async(self) -> Dict[str, Any]:
        """获取队列信息"""
        # 获取排队信息
        total_waiting = await self.redis.llen(self.QUEUE_KEY)
        waiting_users = await self.redis.lrange(self.QUEUE_KEY, 0, -1)

        # 解码用户ID
        waiting_users = [
            user.decode("utf-8") if isinstance(user, bytes) else user
            for user in waiting_users
        ]

        # 获取资源统计
        available_resources = await self.redis.scard(self.FREE_INSTANCES_KEY)
        allocated_resources = await self.redis.hlen(self.ALLOCATIONS_KEY)

        return {
            "total_waiting": total_waiting,
            "total_resources": self.total_instances,
            "available_resources": available_resources,
            "allocated_resources": allocated_resources,
            "waiting_users": waiting_users,
        }

    # === 私有方法 ===

    def _distributed_lock(self, lock_name: str):
        """分布式锁上下文管理器"""
        return RedisDistributedLock(
            self.redis,
            f"{self.LOCK_KEY}:{lock_name}",
            timeout=self.LOCK_TIMEOUT,
        )

    async def _record_allocation(self, user_id: str, instance_id: str):
        """记录分配信息"""
        # 使用管道操作确保原子性
        pipe = self.redis.pipeline()
        pipe.hset(self.ALLOCATIONS_KEY, instance_id, user_id)
        pipe.hset(self.USER_ALLOCATIONS_KEY, user_id, instance_id)
        pipe.expire(self.ALLOCATIONS_KEY, self.ALLOCATION_TTL)
        pipe.expire(self.USER_ALLOCATIONS_KEY, self.ALLOCATION_TTL)
        await pipe.execute()

    async def _clear_allocation(self, user_id: str, instance_id: str):
        """清理分配记录"""
        pipe = self.redis.pipeline()
        pipe.hdel(self.ALLOCATIONS_KEY, instance_id)
        pipe.hdel(self.USER_ALLOCATIONS_KEY, user_id)
        await pipe.execute()

    async def _add_to_queue(self, user_id: str):
        """将用户添加到队列中（兼容旧版Redis）"""
        # 检查用户是否已经在队列中（兼容旧版Redis的实现）
        queue_length = await self.redis.llen(self.QUEUE_KEY)
        if queue_length > 0:
            queue_items = await self.redis.lrange(self.QUEUE_KEY, 0, -1)
            # 手动检查用户是否已在队列中
            user_already_in_queue = False
            for item in queue_items:
                if isinstance(item, bytes) and item.decode("utf-8") == user_id:
                    user_already_in_queue = True
                    break
                elif item == user_id:
                    user_already_in_queue = True
                    break

            # 只有当用户不在队列中时才添加
            if not user_already_in_queue:
                pipe = self.redis.pipeline()
                pipe.lpush(self.QUEUE_KEY, user_id)
                pipe.hset(self.QUEUE_TIMESTAMPS_KEY, user_id, time.time())
                pipe.expire(self.QUEUE_KEY, self.QUEUE_TTL)
                pipe.expire(self.QUEUE_TIMESTAMPS_KEY, self.QUEUE_TTL)
                await pipe.execute()
        else:
            # 队列为空，直接添加用户
            pipe = self.redis.pipeline()
            pipe.lpush(self.QUEUE_KEY, user_id)
            pipe.hset(self.QUEUE_TIMESTAMPS_KEY, user_id, time.time())
            pipe.expire(self.QUEUE_KEY, self.QUEUE_TTL)
            pipe.expire(self.QUEUE_TIMESTAMPS_KEY, self.QUEUE_TTL)
            await pipe.execute()

    async def _wait_for_resource(
        self,
        user_id: str,
        timeout: Optional[float],
    ) -> Tuple[str, AllocationStatus]:
        """等待资源释放（简化实现）"""
        # 加入队列
        await self._add_to_queue(user_id)

        # 这里需要实现基于Redis pub/sub的通知机制
        # 当前简化为立即返回超时
        logger.warning(
            f"[{self.resource_type}] 用户 {user_id} 等待资源超时（简化实现）",
        )
        return "", AllocationStatus.WAIT_TIMEOUT

    async def _notify_queued_users(self):
        """通知排队用户资源可用"""
        # 获取第一个排队用户
        user_id = await self.redis.rpop(self.QUEUE_KEY)
        if user_id:
            if isinstance(user_id, bytes):
                user_id = user_id.decode("utf-8")

            # 清理时间戳记录
            await self.redis.hdel(self.QUEUE_TIMESTAMPS_KEY, user_id)

            # 这里可以发送通知（例如通过pub/sub）
            logger.info(f"[{self.resource_type}] 通知用户 {user_id} 资源可用")


class RedisDistributedLock:
    """Redis分布式锁"""

    def __init__(self, redis_client: redis.Redis, key: str, timeout: int = 10):
        self.redis = redis_client
        self.key = key
        self.timeout = timeout
        self.identifier = str(uuid.uuid4())

    async def __aenter__(self):
        """获取锁"""
        end_time = time.time() + self.timeout
        while time.time() < end_time:
            # 尝试获取锁
            acquired = await self.redis.set(
                self.key,
                self.identifier,
                nx=True,
                ex=self.timeout,
            )
            if acquired:
                return self

            # 短暂等待后重试
            await asyncio.sleep(0.001)

        raise TimeoutError(f"Failed to acquire lock: {self.key}")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """释放锁"""
        # 使用Lua脚本确保只有锁的持有者才能释放
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        await self.redis.eval(script, 1, self.key, self.identifier)
