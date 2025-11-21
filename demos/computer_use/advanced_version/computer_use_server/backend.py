# -*- coding: utf-8 -*-
import asyncio
import time
import os
import json
import requests
import uuid
import weakref
import socket
import datetime
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from dotenv import load_dotenv
from computer_use_agent import (  # noqa E501
    ComputerUseAgent,
)
from agents.agent import AgentRequest
from redis_resource_allocator import (
    AllocationStatus,
)
from cua_utils import init_sandbox
from enum import Enum

# 导入Redis状态管理器
from redis_state_manager import RedisStateManager

# 云设备导入
from agentscope_runtime.sandbox.box.cloud_api.cloud_phone_sandbox import (
    CloudPhoneSandbox,
)
from agentscope_runtime.sandbox.box.cloud_api.cloud_computer_sandbox import (
    CloudComputerSandbox,
)

from agentscope_bricks.utils.logger_util import logger
from pydantic import BaseModel, field_validator, model_validator
from typing import List, Optional
from pydantic import ConfigDict

load_dotenv()

app = FastAPI(title="Computer Use Agent Backend", version="1.0.0")

# CORS配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 资源池配置

# 从环境变量获取PHONE_INSTANCE_IDS，如果未设置则使用默认值
PHONE_INSTANCE_IDS = os.getenv("PHONE_INSTANCE_IDS", "").split(",")
PHONE_INSTANCE_IDS = [id.strip() for id in PHONE_INSTANCE_IDS if id.strip()]


# 从环境变量获取DESKTOP_IDS，如果未设置则使用默认值
DESKTOP_IDS = os.getenv("DESKTOP_IDS", "").split(",")
DESKTOP_IDS = [id.strip() for id in DESKTOP_IDS if id.strip()]


def _serialize(obj):
    """把任意 Python 对象序列化成可打印 / 可 JSON 化的 dict / list / 基础类型"""
    # 1. 基础类型
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj

    # 2. 字典
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}

    # 3. 列表 / 元组 / 集合
    if isinstance(obj, (list, tuple, set)):
        return [_serialize(v) for v in obj]

    # 4. 处理特殊类型
    if isinstance(obj, (bytes, bytearray)):
        try:
            return obj.decode("utf-8")
        except UnicodeDecodeError:
            return f"<bytes: {len(obj)} bytes>"

    if isinstance(obj, datetime.datetime):
        return obj.isoformat()

    if isinstance(obj, (datetime.date, datetime.time)):
        return str(obj)

    # 5. 具有 __dict__ 的普通对象
    if hasattr(obj, "__dict__"):
        try:
            return {k: _serialize(v) for k, v in obj.__dict__.items()}
        except (AttributeError, TypeError) as e:
            return (
                f"<object: {type(obj).__name__} (serialization"
                f" error: {str(e)})>"
            )

    # 6. 具有 __slots__ 的对象
    if hasattr(obj, "__slots__"):
        try:
            return {
                name: _serialize(getattr(obj, name))
                for name in obj.__slots__
                if hasattr(obj, name)
            }
        except (AttributeError, TypeError) as e:
            return (
                f"<object: {type(obj).__name__} (serialization"
                f" error: {str(e)})>"
            )

    # 7. 尝试调用对象的__str__或__repr__
    try:
        if hasattr(obj, "__str__"):
            str_repr = str(obj)
            # 如果字符串表示不太长，直接返回
            if len(str_repr) < 1000:
                return str_repr
            else:
                return f"<{type(obj).__name__}: {str_repr[:100]}...>"
    except Exception:
        pass

    # 8. 其它：返回类型名称
    return f"<{type(obj).__name__}: unable to serialize>"


# utils
def _purge_queue(q: asyncio.Queue):
    while not q.empty():
        try:
            q.get_nowait()
        except asyncio.QueueEmpty:
            break


async def _wait_for_pc_ready(
    equipment,
    desktop_id: str,
    max_wait_time: int = 300,
    stability_check_duration: int = 10,
):
    """
    异步等待PC设备就绪，增加稳定性检查

    Args:
        equipment: CloudComputerSandbox实例
        desktop_id: 桌面ID
        max_wait_time: 最大等待时间（秒），默认5分钟
        stability_check_duration: 稳定性检查时长（秒），确保设备持续运行状态
    """
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
                    logger.info(
                        f"PC {desktop_id} status: running, starting "
                        f"stability check...",
                    )

                # 检查设备是否已稳定运行足够长时间
                stable_duration = time.time() - stable_start_time
                if stable_duration >= stability_check_duration:
                    logger.info(
                        f"✓ PC {desktop_id} is stable and ready "
                        f"(stable for {stable_duration:.1f}s)",
                    )
                    break
                else:
                    logger.info(
                        f"PC {desktop_id} stability check: "
                        f"{stable_duration:.1f}s"
                        f"/{stability_check_duration}s",
                    )
            else:
                # 状态不是运行中，重置稳定性检查
                if stable_start_time is not None:
                    logger.info(
                        f"PC {desktop_id} status changed, "
                        f"resetting stability check",
                    )
                    stable_start_time = None
                current_status = (
                    pc_info[0].desktop_status.lower() if pc_info else "unknown"
                )
                logger.info(
                    f"PC {desktop_id} status: {current_status}, waiting...",
                )
            # 检查是否超时
            if time.time() - start_time > max_wait_time:
                logger.error(
                    f"PC {desktop_id} failed to become ready within"
                    f" {max_wait_time} seconds",
                )

        except Exception as e:
            logger.error(f"Error checking PC status for {desktop_id}: {e}")
            # 出现异常时重置稳定性检查
            stable_start_time = None

        await asyncio.sleep(3)  # 减少检查间隔，更精确的监控


async def _wait_for_phone_ready(
    equipment,
    instance_id: str,
    max_wait_time: int = 300,
):
    """
    异步等待手机设备就绪

    Args:
        equipment: CloudPhoneSandbox实例
        instance_id: 实例ID
        max_wait_time: 最大等待时间（秒），默认5分钟
    """
    start_time = time.time()
    while True:
        try:
            # 将同步的状态检查操作放到线程池中执行
            total_count, next_token, devices_info = await asyncio.to_thread(
                equipment.instance_manager.eds_client.list_instance,
                instance_ids=[instance_id],
            )

            if (
                devices_info
                and devices_info[0].android_instance_status.lower()
                == "running"
            ):
                logger.info(f"✓ Phone {instance_id} is ready")
                break

            # 检查是否超时
            if time.time() - start_time > max_wait_time:
                logger.error(
                    f"Phone {instance_id} failed to become ready within"
                    f" {max_wait_time} seconds",
                )
                raise TimeoutError(
                    f"Phone {instance_id} failed to become ready within"
                    f" {max_wait_time} seconds",
                )

        except Exception as e:
            logger.error(f"Error checking phone status for {instance_id}: {e}")

        await asyncio.sleep(5)


# 环境操作状态枚举
class EnvironmentOperationStatus(Enum):
    """环境操作状态"""

    IDLE = "idle"  # 空闲
    INITIALIZING = "initializing"  # 初始化中
    SWITCHING = "switching"  # 切换中
    COMPLETED = "completed"  # 完成
    FAILED = "failed"  # 失败
    QUEUED = "queued"  # 排队中
    WAITING_RETRY = "waiting_retry"  # 等待重试


class EnvironmentOperation:
    """环境操作任务"""

    def __init__(
        self,
        operation_id: str,
        operation_type: str,
        config: dict,
        user_id: str,
        chat_id: str,
    ):
        self.operation_id = operation_id
        self.operation_type = operation_type  # "init" or "switch"
        self.config = config
        self.user_id = user_id
        self.chat_id = chat_id
        self.status = EnvironmentOperationStatus.IDLE
        self.message = ""
        self.progress = 0  # 0-100
        self.start_time = None
        self.end_time = None
        self.result = None
        self.error = None
        self.background_task = None

    def to_dict(self):
        return {
            "operation_id": self.operation_id,
            "operation_type": self.operation_type,
            "status": self.status.value,
            "message": self.message,
            "progress": self.progress,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "result": _serialize(self.result) if self.result else None,
            "error": self.error,
        }


# 全局状态管理器
state_manager = None

# 机器标识
MACHINE_ID = os.getenv("MACHINE_ID", socket.gethostname())


# Redis发布订阅控制信号
async def publish_control_signal(
    user_id: str,
    chat_id: str,
    action: str,
    **kwargs,
):
    """发布控制信号到Redis"""
    if not state_manager:
        return False

    try:
        channel = f"control:{user_id}:{chat_id}"
        signal_data = {
            "action": action,
            "timestamp": time.time(),
            "machine_id": MACHINE_ID,
            **kwargs,
        }

        await state_manager.redis_client.publish(
            channel,
            json.dumps(signal_data),
        )
        logger.info(f"发布控制信号: {action} to {channel}")
        return True
    except Exception as e:
        logger.error(f"发布控制信号失败: {e}")
        return False


async def listen_for_control_signals(user_id: str, chat_id: str, agent):
    """监听Redis控制信号的后台任务"""
    if not state_manager:
        return

    channel = f"control:{user_id}:{chat_id}"
    pubsub = state_manager.redis_client.pubsub()

    try:
        await pubsub.subscribe(channel)
        logger.info(f"开始监听控制信号: {channel}")

        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    signal_data = json.loads(message["data"])
                    action = signal_data.get("action")

                    logger.info(f"收到控制信号: {action} from {channel}")

                    if action == "stop":
                        # 设置停止标志
                        if hasattr(agent, "should_stop"):
                            agent.should_stop = True
                        # 更新Redis状态
                        await state_manager.update_chat_state(
                            user_id,
                            chat_id,
                            {"is_running": False, "stop_requested": True},
                        )
                    elif action == "interrupt_wait":
                        # 调用中断等待方法
                        if hasattr(agent, "interrupt_wait"):
                            agent.interrupt_wait()

                except Exception as e:
                    logger.error(f"处理控制信号时出错: {e}")

    except Exception as e:
        logger.error(f"监听控制信号失败: {e}")
    finally:
        try:
            await pubsub.unsubscribe(channel)
            await pubsub.close()
        except Exception as e:
            logger.error(f"关闭pub/sub连接失败: {e}")


async def check_stop_signal_from_redis(user_id: str, chat_id: str) -> bool:
    """检查Redis中的停止信号"""
    try:
        chat_state = await state_manager.get_chat_state(user_id, chat_id)
        if isinstance(chat_state, dict):
            return not chat_state.get("is_running", True) or chat_state.get(
                "stop_requested",
                False,
            )
        else:
            return not getattr(chat_state, "is_running", True) or getattr(
                chat_state,
                "stop_requested",
                False,
            )
    except Exception as e:
        logger.error(f"检查停止信号失败: {e}")
        return False


# Redis操作超时保护函数
async def safe_redis_operation(
    operation_func,
    *args,
    timeout=10.0,
    max_retries=3,
    **kwargs,
):
    """
    安全的Redis操作，带重试和超时保护

    Args:
        operation_func: Redis操作函数
        timeout: 超时时间（秒）
        max_retries: 最大重试次数
        *args, **kwargs: 传递给操作函数的参数

    Returns:
        操作结果，如果失败返回None
    """
    for attempt in range(max_retries):
        try:
            return await asyncio.wait_for(
                operation_func(*args, **kwargs),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.warning(f"Redis操作超时，尝试 {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1.0 * (attempt + 1))  # 递增延迟
            else:
                logger.error("Redis操作最终超时失败")
                return None
        except Exception as e:
            logger.warning(
                f"Redis操作失败，尝试 {attempt + 1}/{max_retries}: {e}",
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(1.0 * (attempt + 1))
            else:
                logger.error(f"Redis操作最终失败: {e}")
                return None
    return None


if not hasattr(app.state, "running_agents"):
    app.state.running_agents = weakref.WeakValueDictionary()


async def validate_user_session(
    user_id: str,
    chat_id: str,
    strict_mode: bool = True,
) -> bool:
    """
    验证用户会话的有效性

    Args:
        user_id: 用户ID
        chat_id: 会话ID
        strict_mode: 严格模式下，非活跃会话会拒绝请求；非严格模式下，仅记录警告

    Returns:
        bool: 会话是否有效
    """
    try:
        is_valid = await state_manager.validate_user_active_chat(
            user_id,
            chat_id,
        )

        if not is_valid:
            if strict_mode:
                logger.info(
                    f"Rejected request: Invalid chat session for user "
                    f"{user_id}, chat {chat_id}",
                )
                raise HTTPException(
                    status_code=403,
                    detail={
                        "message": "Invalid chat session. Another session "
                        "may be active for this user.",
                        "user_id": user_id,
                        "chat_id": chat_id,
                        "type": "session_conflict",
                    },
                )
            else:
                logger.warning(
                    f"⚠️ Warning: Invalid chat session for user "
                    f"{user_id}, chat {chat_id}",
                )

        return is_valid

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Session validation error for {user_id}:{chat_id}: {e}")
        if strict_mode:
            raise HTTPException(
                status_code=500,
                detail="Session validation failed",
            )
        return False


# 请求/响应模型
class MessageContent(BaseModel):
    type: str
    text: Optional[str] = None
    image: Optional[str] = None


class Message(BaseModel):
    role: str
    content: List[MessageContent]


class AgentConfig(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    mode: str = "qwen_vl"
    sandbox_type: str = "e2b-desktop"
    save_logs: bool = True
    timeout: int = 120
    pc_use_addon_info: str = ""
    max_steps: int = 20
    user_id: str = ""
    chat_id: str = ""
    e2e_info: list = []
    extra_params: str = ""

    @model_validator(mode="after")
    def set_defaults_based_on_mode(self):
        # 只有当 sandbox_type 是默认值时才根据 mode 设置新值
        if self.sandbox_type == "e2b-desktop":  # 默认值
            if self.mode == "pc_use":
                self.sandbox_type = "pc_wuyin"
            elif self.mode == "phone_use":
                self.sandbox_type = "phone_wuyin"
        return self

    @field_validator("e2e_info", mode="before")
    @classmethod
    def set_e2e_info_defaults(cls, v, info):
        mode = info.data.get("mode") if "mode" in info.data else "qwen_vl"
        if mode == "pc_use":
            default_e2e_info = [
                {"pipeline_name": "pre-pc-agent-e"},
                {"pipeline_type": "agent"},
                {"use_add_info_generate": "false"},
                {"model_name": "pre-gui_owl_7b"},
                {"device_type": "pc"},
            ]

            # 如果没有提供e2e_info或者为空，则使用默认值
            if not v:
                return default_e2e_info
            else:
                # 如果提供了e2e_info，合并默认值和提供的值
                if isinstance(v, list):
                    # 创建默认值字典以便查找
                    default_dict = {}
                    for item in default_e2e_info:
                        default_dict.update(item)

                    # 合并逻辑：将用户提供的字典与默认字典合并
                    merged = []
                    user_dict = {}
                    for item in v:
                        if isinstance(item, dict):
                            user_dict.update(item)

                    # 对于每个默认键，如果用户提供了该键且值不为空，则使用用户值，否则使用默认值
                    for key, default_value in default_dict.items():
                        user_value = user_dict.get(key)
                        # 当用户传入的值为空（None, "", 空列表等）时，使用默认值
                        if user_value is not None and user_value != "":
                            merged.append({key: user_value})
                        else:
                            merged.append({key: default_value})
                    return merged
                else:
                    return default_e2e_info
        elif mode == "phone_use":
            default_e2e_info = [
                {"device_type": "mobile"},
                {"model_name": "pre-gui_owl_7b"},
                {"pipeline_type": "agent"},
                {"pipeline_name": "mobile-agent-pipeline"},
                {"use_add_info_generate": "false"},
            ]

            # 如果没有提供e2e_info或者为空，则使用默认值
            if not v:
                return default_e2e_info
            else:
                # 如果提供了e2e_info，合并默认值和提供的值
                if isinstance(v, list):
                    # 创建默认值字典以便查找
                    default_dict = {}
                    for item in default_e2e_info:
                        default_dict.update(item)

                    # 合并逻辑：将用户提供的字典与默认字典合并
                    merged = []
                    user_dict = {}
                    for item in v:
                        if isinstance(item, dict):
                            user_dict.update(item)

                    # 对于每个默认键，如果用户提供了该键且值不为空，则使用用户值，否则使用默认值
                    for key, default_value in default_dict.items():
                        user_value = user_dict.get(key)
                        # 当用户传入的值为空（None, "", 空列表等）时，使用默认值
                        if user_value is not None and user_value != "":
                            merged.append({key: user_value})
                        else:
                            merged.append({key: default_value})
                    return merged
                else:
                    return default_e2e_info
        # 如果v不为空但mode不是pc_use或phone_use，则直接返回原始值
        return v if v else []


class ComputerUseRequest(AgentRequest):
    """扩展的Agent请求，包含ComputerUse特定配置"""

    config: Optional[AgentConfig] = AgentConfig()
    sequence_number: Optional[int] = None  # 新增：序列号参数，用于断线续传
    user_id: str = ""


class RunRequest(BaseModel):
    messages: List[Message]
    config: Optional[AgentConfig] = AgentConfig()


class InitRequest(BaseModel):
    config: Optional[AgentConfig] = AgentConfig()
    user_id: str = ""


class UserChatRequest(BaseModel):
    user_id: str
    chat_id: str


async def get_equipment(
    user_id: str,
    chat_id: str,
    config: AgentConfig,
) -> dict:
    """
    设备查询
    注意：这里重新使用锁，因为 run_task 已经在锁外调用
    """
    chat_state = await state_manager.get_chat_state(user_id, chat_id)

    # 📝 恢复使用锁，确保设备初始化的原子性
    async with chat_state.lock:
        sandbox_type = config.sandbox_type
        task_id = str(uuid.uuid4())
        static_url = config.static_url
        logger.info(
            f"启动中: {chat_id}-{sandbox_type}\n",
        )

        if chat_state.equipment is not None:
            logger.info(f"设备已存在: {chat_id}-{sandbox_type}")
            if sandbox_type == "pc_wuyin":
                instance_m = chat_state.equipment.instance_manager

                # 验证PC设备状态和auth_code有效性
                try:
                    # 检查设备状态
                    pc_info = await asyncio.to_thread(
                        instance_m.ecd_client.search_desktop_info,
                        [instance_m.desktop_id],
                    )

                    if (
                        not pc_info
                        or pc_info[0].desktop_status.lower() != "running"
                    ):
                        logger.warning(
                            f"PC设备状态异常，重新初始化: {instance_m.desktop_id}",
                        )
                        # 清理旧设备
                        chat_state.equipment = None
                        # 递归调用，重新初始化设备
                        return await get_equipment(user_id, chat_id, config)

                    # 重新获取auth_code确保有效性
                    logger.info(
                        f"重新获取auth_code确保有效性，desktop_id: "
                        f"{instance_m.desktop_id}",
                    )
                    new_auth_code = (
                        await instance_m.app_stream_client.search_auth_code()
                    )
                    if new_auth_code:
                        instance_m.auth_code = new_auth_code
                        logger.info(
                            f"auth_code已更新: {instance_m.desktop_id}",
                        )
                    else:
                        logger.warning(
                            f"获取新auth_code失败，使用原有auth_code: "
                            f"{instance_m.desktop_id}",
                        )

                except Exception as e:
                    logger.error(f"验证PC设备时出错: {e}，重新初始化设备")
                    # 清理旧设备
                    chat_state.equipment = None
                    # 递归调用，重新初始化设备
                    return await get_equipment(user_id, chat_id, config)

                logger.info(
                    f"设备已存在，返回设备信息-desktop_id: {instance_m.desktop_id}",
                )
                return {
                    "task_id": task_id,
                    "equipment_web_url": chat_state.equipment_web_url,
                    "equipment_web_sdk_info": {
                        "auth_code": instance_m.auth_code,
                        "desktop_id": instance_m.desktop_id,
                        "static_url": static_url,
                    },
                }
            elif sandbox_type == "phone_wuyin":
                instance_m = chat_state.equipment.instance_manager

                # 验证手机设备状态和ticket有效性
                try:
                    # 检查设备状态
                    total_count, next_token, devices_info = (
                        await asyncio.to_thread(
                            instance_m.eds_client.list_instance,
                            instance_ids=[instance_m.instance_id],
                        )
                    )

                    if (
                        not devices_info
                        or devices_info[0].android_instance_status.lower()
                        != "running"
                    ):
                        logger.warning(
                            f"手机设备状态异常，重新初始化: {instance_m.instance_id}",
                        )
                        # 清理旧设备
                        chat_state.equipment = None
                        # 递归调用，重新初始化设备
                        return await get_equipment(user_id, chat_id, config)

                    # 对于手机设备，可能需要重新获取ticket（如果有相应的接口）
                    # 这里暂时保持原有逻辑，如果需要可以添加类似PC的ticket刷新

                except Exception as e:
                    logger.error(f"验证手机设备时出错: {e}，重新初始化设备")
                    # 清理旧设备
                    chat_state.equipment = None
                    # 递归调用，重新初始化设备
                    return await get_equipment(user_id, chat_id, config)

                logger.info(
                    f"设备已存在，返回设备信息-instance_id: {instance_m.instance_id}",
                )
                return {
                    "task_id": task_id,
                    "equipment_web_url": chat_state.equipment_web_url,
                    "equipment_web_sdk_info": {
                        "ticket": instance_m.ticket,
                        "person_app_id": instance_m.person_app_id,
                        "app_instance_id": instance_m.instance_id,
                        "static_url": static_url,
                    },
                }
        else:
            # 初始化PC设备
            if sandbox_type == "pc_wuyin":
                # 从资源分配器获取PC实例
                logger.info(
                    f"启动user_id: {user_id}, chat_id: {chat_id}",
                )
                desktop_id, status = (
                    await state_manager.pc_allocator.allocate_async(
                        f"{user_id}:{chat_id}",
                        timeout=0,
                    )
                )
                logger.info(
                    f"启动desktop_id: {desktop_id}, status: {status}",
                )
                # 处理资源排队情况
                if status == AllocationStatus.WAIT_TIMEOUT:
                    pc_a = state_manager.pc_allocator
                    position = (
                        await pc_a.get_chat_position(
                            f"{user_id}:{chat_id}",
                        )
                    )[0]
                    total_waiting = (await pc_a.get_queue_info_async())[
                        "total_waiting"
                    ]
                    raise HTTPException(
                        status_code=429,
                        detail={
                            "message": "All PC resources are currently in use",
                            "queue_position": position + 1,
                            "total_waiting": total_waiting,
                            "type": "queued",
                        },
                    )

                if not (
                    status == AllocationStatus.SUCCESS
                    or status == AllocationStatus.CHAT_ALREADY_ALLOCATED
                ):
                    logger.error(f"Failed to allocate PC resource: {status}")
                    raise HTTPException(
                        503,
                        "Failed " "to allocate PC resource",
                    )

                # 创建云电脑实例
                try:
                    equipment = await asyncio.to_thread(
                        CloudComputerSandbox,
                        desktop_id=desktop_id,
                    )
                except Exception as e:
                    # 初始化失败时释放已分配的资源
                    logger.error(
                        f"CloudComputerSandbox初始化失败: {e}，释放资源 {desktop_id}",
                    )
                    await state_manager.pc_allocator.release_async(desktop_id)
                    raise HTTPException(
                        503,
                        f"Failed to initialize PC resource: {str(e)}",
                    )

                logger.info(
                    f"启动equipment: {equipment}-{equipment.instance_manager}"
                    f"-{equipment.instance_manager.ecd_client}",
                )
                # 更新对话状态
                chat_state.equipment = equipment
                chat_state.task_id = task_id
                chat_state.sandbox_type = sandbox_type
                chat_state.equipment_web_url = (
                    f"{static_url}equipment_computer.html"
                )
                logger.info(
                    f"启动instance_id: {desktop_id}\n",
                )
                return {
                    "task_id": task_id,
                    "equipment_web_url": chat_state.equipment_web_url,
                    "equipment_web_sdk_info": {
                        "auth_code": equipment.instance_manager.auth_code,
                        "desktop_id": desktop_id,
                        "static_url": static_url,
                    },
                }

            # 初始化手机设备
            elif sandbox_type == "phone_wuyin":
                # 从资源分配器获取手机实例
                instance_id, status = (
                    await state_manager.phone_allocator.allocate_async(
                        f"{user_id}:{chat_id}",
                        timeout=0,
                    )
                )
                logger.info(
                    f"启动 instance_id: {instance_id}, status: {status}",
                )
                # 处理资源排队情况
                if status == AllocationStatus.WAIT_TIMEOUT:
                    pc_a = state_manager.phone_allocator
                    position = (
                        await pc_a.get_chat_position(
                            f"{user_id}:{chat_id}",
                        )
                    )[0]
                    total_waiting = (await pc_a.get_queue_info_async())[
                        "total_waiting"
                    ]
                    logger.warning(
                        f"Failed to allocate phone resource: {status}",
                    )
                    raise HTTPException(
                        status_code=429,
                        detail={
                            "message": "All phone resources are "
                            "currently in use",
                            "queue_position": position + 1,
                            "total_waiting": total_waiting,
                            "type": "queued",
                        },
                    )

                if not (
                    status == AllocationStatus.SUCCESS
                    or status == AllocationStatus.CHAT_ALREADY_ALLOCATED
                ):
                    logger.warning(
                        f"Failed to allocate phone resource: {status}",
                    )
                    raise HTTPException(
                        503,
                        "Failed to allocate phone resource",
                    )

                # 创建云手机设备对象 - 异步初始化
                try:
                    equipment = await asyncio.to_thread(
                        CloudPhoneSandbox,
                        instance_id=instance_id,
                    )
                except Exception as e:
                    # 🚨 初始化失败时释放已分配的资源
                    logger.error(
                        f"CloudPhoneSandbox初始化失败: {e}，释放资源 {instance_id}",
                    )
                    await state_manager.phone_allocator.release_async(
                        instance_id,
                    )
                    raise HTTPException(
                        503,
                        f"Failed to initialize "
                        f"phone "
                        f"resource: {str(e)}",
                    )
                # 更新对话状态
                chat_state.equipment = equipment
                chat_state.task_id = task_id
                chat_state.sandbox_type = sandbox_type
                chat_state.equipment_web_url = (
                    f"{static_url}equipment_phone.html"
                )
                logger.info(f"启动instance_id: {instance_id}")
                e_in_m = equipment.instance_manager
                return {
                    "task_id": task_id,
                    "equipment_web_url": chat_state.equipment_web_url,
                    "equipment_web_sdk_info": {
                        "ticket": e_in_m.ticket,
                        "person_app_id": e_in_m.person_app_id,
                        "app_instance_id": e_in_m.instance_id,
                        "static_url": static_url,
                    },
                }

            # 默认e2b桌面初始化
            else:
                equipment = await asyncio.to_thread(init_sandbox)
                chat_state.equipment = equipment
                chat_state.sandbox = equipment.device
                chat_state.task_id = task_id
                chat_state.sandbox_type = sandbox_type

                return {
                    "task_id": task_id,
                    "sandbox_url": (
                        equipment.device.stream.get_url()
                        if equipment.device
                        else None
                    ),
                }


async def _handle_resume_stream(
    user_id: str,
    chat_id: str,
    from_sequence: int,
):
    """处理断线续传，返回历史数据"""
    logger.info(
        f"处理断线续传，用户: {user_id}, 对话: {chat_id}, 从序列号: {from_sequence}",
    )

    async def resume_stream():
        try:
            # 获取对话状态，确定task_id
            chat_state = await state_manager.get_chat_state(user_id, chat_id)
            current_task_id = chat_state.get("task_id")

            # 从Redis获取历史数据
            historical_data = (
                await state_manager.get_stream_data_from_sequence(
                    user_id,
                    chat_id,
                    from_sequence,
                    current_task_id,
                )
            )

            logger.info(f"找到 {len(historical_data)} 条历史数据")
            # 发送历史数据
            for data_item in historical_data:
                json_str = json.dumps(data_item, ensure_ascii=False)
                yield f"data: {json_str}\n\n"

            # 如果对话当前有正在运行的任务，继续流式输出新数据
            if chat_state.get("is_running") and chat_state.get("agent"):
                logger.info("检测到正在运行任务，继续流式输出新数据")
                # 监听新数据（这里需要实现一个机制来获取实时数据）
                # 暂时先获取最新的序列号，等待新数据
                latest_sequence = (
                    await state_manager.get_latest_sequence_number(
                        user_id,
                        chat_id,
                        current_task_id,
                    )
                )

                # 如果最新序列号大于请求的序列号，说明有新数据
                if latest_sequence > from_sequence:
                    new_data = (
                        await state_manager.get_stream_data_from_sequence(
                            user_id,
                            chat_id,
                            max(
                                from_sequence,
                                latest_sequence - 10,
                            ),  # 获取最近的数据
                            current_task_id,
                        )
                    )

                    for data_item in new_data:
                        if data_item.get("sequence_number", 0) > from_sequence:
                            json_str = json.dumps(
                                data_item,
                                ensure_ascii=False,
                            )
                            yield f"data: {json_str}\n\n"

        except Exception as e:
            logger.error(f"处理断线续传时出错: {e}")
            # 对于断线续传错误，我们仍然手动创建（不存储到Redis，因为这不是agent数据）
            error_data = {
                "sequence_number": from_sequence,
                "object": "error",
                "status": "error",
                "error": f"Resume failed: {str(e)}",
                "type": "error",
                "data": {"error": str(e)},
            }
            yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        resume_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true",
            "X-Accel-Buffering": "no",  # 禁用nginx缓冲
            "Content-Type": "text/event-stream; charset=utf-8",
            "Transfer-Encoding": "chunked",
            "Keep-Alive": "timeout=300, max=1000",  # 设置keep-alive参数
        },
    )


async def _handle_new_stream(
    user_id: str,
    chat_id: str,
    request: ComputerUseRequest,
):
    """处理新的流式任务"""
    logger.info(f"开始新任务，用户: {user_id}, 对话: {chat_id}")

    async def agent_stream():
        """Agent流式响应生成器 - 支持序列号存储，优化错误处理"""
        task_id = None
        agent = None
        async_iterator = None
        background_tasks = []  # 后台任务列表

        def safe_json_dumps(obj, default=None):
            """安全的JSON序列化，处理特殊类型"""
            try:
                return json.dumps(obj, ensure_ascii=False, default=default)
            except (TypeError, ValueError) as e:
                logger.warning(f"JSON序列化失败，使用fallback: {e}")
                # 尝试使用自定义序列化
                try:
                    return json.dumps(_serialize(obj), ensure_ascii=False)
                except Exception as fallback_error:
                    logger.error(f"Fallback序列化也失败: {fallback_error}")
                    return json.dumps(
                        {
                            "error": "序列化失败",
                            "error_type": str(type(obj).__name__),
                            "error_message": str(fallback_error),
                        },
                        ensure_ascii=False,
                    )

        async def store_to_redis_background(
            user_id: str,
            chat_id: str,
            data: dict,
            task_id: str,
        ):
            """后台存储到Redis，不阻塞流式输出"""
            try:
                await safe_redis_operation(
                    state_manager.store_stream_data,
                    user_id,
                    chat_id,
                    data,
                    task_id,
                    timeout=5.0,
                    max_retries=1,
                )
            except Exception as bg_error:
                logger.warning(f"后台Redis存储失败: {bg_error}")

        try:
            # 创建AgentScope Context模拟对象
            class MockContext:
                def __init__(self, request):
                    self.request = request

            context = MockContext(request)

            # 清空旧的流式数据（带超时保护）
            try:
                await asyncio.wait_for(
                    state_manager.clear_stream_data(user_id, chat_id),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                logger.warning("清空流式数据超时，继续执行")
            except Exception as clear_error:
                logger.warning(f"清空流式数据失败: {clear_error}")

            # 设置任务状态（带超时保护）
            try:
                await asyncio.wait_for(
                    state_manager.update_chat_state(
                        user_id,
                        chat_id,
                        {
                            "is_running": True,
                            "current_task": f"Agent API Task from input:"
                            f" {len(request.input)} messages",
                        },
                    ),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                logger.warning("更新任务状态超时，继续执行")
            except Exception as state_error:
                logger.warning(f"更新任务状态失败: {state_error}")

            # 重新获取更新后的state
            try:
                chat_state = await asyncio.wait_for(
                    state_manager.get_chat_state(user_id, chat_id),
                    timeout=5.0,
                )
                task_id = chat_state.get("task_id")
            except asyncio.TimeoutError:
                logger.error("获取chat_state超时")
                raise
            except Exception as get_state_error:
                logger.error(f"获取chat_state失败: {get_state_error}")
                raise

            # 创建Agent配置
            agent_config = {
                "equipment": chat_state.get("equipment"),
                "output_dir": ".",
                "sandbox_type": chat_state.get("sandbox_type"),
                "status_callback": None,
                "mode": (
                    "phone_use"
                    if chat_state.get("sandbox_type") == "phone_wuyin"
                    else "pc_use"
                ),
                "pc_use_add_info": request.config.pc_use_addon_info,
                "max_steps": request.config.max_steps,
                "chat_id": chat_id,
                "user_id": user_id,
                "e2e_info": request.config.e2e_info,
                "extra_params": request.config.extra_params,
                "state_manager": state_manager,
            }

            # 创建Agent实例
            agent = ComputerUseAgent(
                name="ComputerUseAgent",
                agent_config=agent_config,
            )

            # 将agent引用存储在应用状态中，支持多实例部署
            app.state.running_agents[f"{user_id}:{chat_id}"] = agent

            # 将agent标识存储到Redis（仅用于状态检查，不是真实对象）
            try:
                await asyncio.wait_for(
                    state_manager.update_chat_state(
                        user_id,
                        chat_id,
                        {
                            "agent_running": True,
                            "agent_id": id(agent),  # 存储agent的标识符
                        },
                    ),
                    timeout=5.0,
                )
            except Exception as agent_state_error:
                logger.warning(f"存储agent状态失败: {agent_state_error}")

            logger.info(f"开始Agent执行，用户: {user_id}, 对话: {chat_id}")

            # 心跳机制变量（仅用于内部检测，不发送给客户端）
            last_heartbeat = time.time()
            heartbeat_interval = 30  # 30秒心跳间隔
            last_data_time = time.time()  # 最后数据时间
            max_idle_time = 300  # 最大空闲时间（5分钟）

            try:
                async_iterator = agent.run_async(context)
                async for result in async_iterator:
                    # 检查客户端是否断开（通过检查停止信号）
                    try:
                        if await asyncio.wait_for(
                            state_manager.check_stop_signal(user_id, chat_id),
                            timeout=0.1,
                        ):
                            logger.info("检测到停止信号，终止流式输出")
                            break
                    except (asyncio.TimeoutError, Exception):
                        pass  # 忽略检查超时或错误，继续执行

                    # 检查是否空闲超时
                    current_time = time.time()
                    if current_time - last_data_time > max_idle_time:
                        logger.warning("流式输出空闲超时，终止连接")
                        idle_error = {
                            "sequence_number": None,
                            "object": "error",
                            "status": "error",
                            "error": "流式输出空闲超时",
                            "type": "timeout_error",
                        }
                        yield f"data: {safe_json_dumps(idle_error)}\n\n"
                        break

                    last_data_time = current_time

                    # 内部心跳检测（仅用于连接状态检测，不发送给客户端）
                    if current_time - last_heartbeat >= heartbeat_interval:
                        # 仅更新心跳时间，不发送心跳数据给客户端
                        # 这样可以保持数据结构一致，同时仍然可以检测连接状态
                        last_heartbeat = current_time
                        # 可选：后台记录心跳到Redis（用于监控），但不发送给客户端
                        # heartbeat_data = {
                        #     "object": "heartbeat",
                        #     "type": "heartbeat",
                        #     "timestamp": current_time,
                        #     "status": "alive",
                        #     "user_id": user_id,
                        #     "chat_id": chat_id,
                        # }
                        # bg_task = asyncio.create_task(
                        #     store_to_redis_background(
                        #         user_id, chat_id, heartbeat_data, task_id
                        #     )
                        # )
                        # background_tasks.append(bg_task)

                    try:
                        # 将Agent的输出转换为JSON格式
                        try:
                            if hasattr(result, "model_dump"):
                                result_dict = result.model_dump()
                            else:
                                result_dict = _serialize(result)
                        except Exception as serialize_error:
                            logger.error(
                                f"序列化result失败: {serialize_error}",
                            )
                            # 创建错误数据
                            result_dict = {
                                "error": f"序列化失败: {str(serialize_error)}",
                                "type": "serialization_error",
                                "raw_result_type": str(type(result).__name__),
                            }

                        # 后台存储到Redis（不阻塞流式输出）
                        result_dict_copy = result_dict.copy()
                        bg_task = asyncio.create_task(
                            store_to_redis_background(
                                user_id,
                                chat_id,
                                result_dict_copy,
                                task_id,
                            ),
                        )
                        background_tasks.append(bg_task)

                        # 先发送数据，序列号稍后更新（如果需要可以异步更新）
                        result_dict["sequence_number"] = None
                        result_dict["storage_status"] = "pending"

                        try:
                            json_str = safe_json_dumps(result_dict)
                            yield f"data: {json_str}\n\n"
                        except Exception as yield_error:
                            logger.error(f"yield数据失败: {yield_error}")
                            # 发送错误消息
                            error_msg = {
                                "sequence_number": None,
                                "object": "error",
                                "status": "error",
                                "error": f"发送数据失败: {str(yield_error)}",
                                "type": "yield_error",
                            }
                            yield f"data: {safe_json_dumps(error_msg)}\n\n"
                            continue

                    except Exception as process_error:
                        logger.error(
                            f"处理输出时出错: {process_error}",
                            exc_info=True,
                        )
                        # 创建错误数据
                        error_data = {
                            "sequence_number": None,
                            "object": "error",
                            "status": "error",
                            "error": f"处理输出时出错: {str(process_error)}",
                            "type": "processing_error",
                        }

                        # 后台存储错误
                        bg_task = asyncio.create_task(
                            store_to_redis_background(
                                user_id,
                                chat_id,
                                error_data,
                                task_id,
                            ),
                        )
                        background_tasks.append(bg_task)

                        # 立即发送错误
                        try:
                            error_json = safe_json_dumps(error_data)
                            yield f"data: {error_json}\n\n"
                        except Exception as error_yield_error:
                            logger.error(
                                f"发送错误消息失败: {error_yield_error}",
                            )
                        continue

            except asyncio.CancelledError:
                logger.info(
                    f"Agent流式输出被取消，用户: {user_id}, 对话: {chat_id}",
                )
                # 发送取消消息
                cancel_msg = {
                    "sequence_number": None,
                    "object": "error",
                    "status": "cancelled",
                    "error": "任务已取消",
                    "type": "cancelled",
                }
                try:
                    yield f"data: {safe_json_dumps(cancel_msg)}\n\n"
                except Exception:
                    pass
                raise
            except GeneratorExit:
                logger.info(
                    f"Agent流式输出生成器退出，用户: {user_id}, 对话: {chat_id}",
                )
                raise
            except Exception as iteration_error:
                logger.error(
                    f"Agent执行时出错: {iteration_error}",
                    exc_info=True,
                )
                # 创建错误数据
                error_data = {
                    "sequence_number": None,
                    "object": "error",
                    "status": "error",
                    "error": f"Agent执行时出错: {str(iteration_error)}",
                    "type": "iteration_error",
                }

                # 后台存储错误
                try:
                    bg_task = asyncio.create_task(
                        store_to_redis_background(
                            user_id,
                            chat_id,
                            error_data,
                            task_id,
                        ),
                    )
                    background_tasks.append(bg_task)
                except Exception:
                    pass

                # 立即发送错误
                try:
                    error_json = safe_json_dumps(error_data)
                    yield f"data: {error_json}\n\n"
                except Exception as error_yield_error:
                    logger.error(f"发送迭代错误消息失败: {error_yield_error}")

            finally:
                # 清理资源
                try:
                    if async_iterator and hasattr(async_iterator, "aclose"):
                        await asyncio.wait_for(
                            async_iterator.aclose(),
                            timeout=5.0,
                        )
                except (asyncio.TimeoutError, Exception) as close_error:
                    logger.warning(f"关闭异步迭代器时出错: {close_error}")

                # 等待后台任务完成（最多等待3秒）
                if background_tasks:
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(
                                *background_tasks,
                                return_exceptions=True,
                            ),
                            timeout=3.0,
                        )
                    except asyncio.TimeoutError:
                        logger.warning("后台任务未在超时内完成，继续清理")
                    except Exception as bg_wait_error:
                        logger.warning(f"等待后台任务时出错: {bg_wait_error}")

            logger.info(f"Agent执行完成，用户: {user_id}, 对话: {chat_id}")

        except Exception as e:
            logger.error(
                f"Agent stream execution failed for user {user_id}, "
                f"chat {chat_id}: {e}",
                exc_info=True,
            )
            # 创建错误数据
            error_data = {
                "sequence_number": None,
                "object": "error",
                "status": "error",
                "error": f"任务执行失败: {str(e)}",
                "type": "agent_error",
            }

            try:
                # 尝试后台存储错误
                try:
                    bg_task = asyncio.create_task(
                        store_to_redis_background(
                            user_id,
                            chat_id,
                            error_data,
                            task_id,
                        ),
                    )
                    background_tasks.append(bg_task)
                except Exception:
                    pass

                # 立即发送错误
                error_json = safe_json_dumps(error_data)
                yield f"data: {error_json}\n\n"
            except Exception as final_error:
                logger.error(f"发送最终错误消息失败: {final_error}")
                # 最后的降级方案
                try:
                    final_error_msg = json.dumps(
                        {
                            "sequence_number": None,
                            "object": "error",
                            "status": "error",
                            "error": f"任务执行失败: {str(e)}",
                            "type": "fatal_error",
                        },
                        ensure_ascii=False,
                    )
                    yield f"data: {final_error_msg}\n\n"
                except Exception:
                    pass  # 如果连这个都失败，只能放弃

        finally:
            # 清理状态
            try:
                # 清理agent引用
                composite_key = f"{user_id}:{chat_id}"
                if composite_key in app.state.running_agents:
                    del app.state.running_agents[composite_key]

                # 更新状态（带超时保护）
                try:
                    await asyncio.wait_for(
                        state_manager.update_chat_state(
                            user_id,
                            chat_id,
                            {
                                "is_running": False,
                                "current_task": None,
                                "agent_running": False,
                                "agent_id": None,
                            },
                        ),
                        timeout=5.0,
                    )
                except (
                    asyncio.TimeoutError,
                    Exception,
                ) as cleanup_state_error:
                    logger.warning(f"清理状态时出错: {cleanup_state_error}")

            except Exception as cleanup_error:
                logger.error(f"清理资源时出错: {cleanup_error}", exc_info=True)

    return StreamingResponse(
        agent_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true",
            "X-Accel-Buffering": "no",  # 禁用nginx缓冲
            "Content-Type": "text/event-stream; charset=utf-8",
            "Transfer-Encoding": "chunked",
            "Keep-Alive": "timeout=300, max=1000",  # 设置keep-alive参数
        },
    )


@app.on_event("startup")
async def startup_event():
    global state_manager
    # 使用Redis状态管理器
    state_manager = RedisStateManager(
        phone_instance_ids=PHONE_INSTANCE_IDS,
        desktop_ids=DESKTOP_IDS,
    )
    await state_manager.initialize()

    # 同步实例ID配置到Redis，确保与环境变量保持一致
    logger.info("Synchronizing instance IDs with environment variables")
    await state_manager.sync_instance_ids(
        phone_instance_ids=PHONE_INSTANCE_IDS,
        desktop_ids=DESKTOP_IDS,
    )

    # 启动心跳监控任务
    asyncio.create_task(state_manager._monitor_heartbeats())

    # 注册当前机器到Redis
    try:
        await state_manager.redis_client.hset(
            "machine_registry",
            MACHINE_ID,
            json.dumps(
                {
                    "machine_id": MACHINE_ID,
                    "startup_time": time.time(),
                    "pid": os.getpid(),
                },
            ),
        )
        logger.info(f"机器 {MACHINE_ID} 已注册到Redis")
    except Exception as e:
        logger.error(f"注册机器信息失败: {e}")

    logger.info(
        "Backend startup completed with Redis "
        f"state manager on machine {MACHINE_ID}",
    )


@app.post("/cua/init")
async def init_task(request: InitRequest, user_id: str = ""):
    """触发异步环境初始化"""
    if not user_id:
        if request.user_id:
            user_id = request.user_id
        else:
            user_id = request.config.user_id
    logger.info(f"start init by user_id:{user_id}")
    chat_id = request.config.chat_id
    if not user_id or not chat_id:
        raise HTTPException(
            status_code=400,
            detail="user_id and chat_id are required",
        )
    logger.info(
        f"接收到任务请求，用户: {user_id}, 对话: {chat_id} "
        f"request: {json.dumps(request.model_dump(), ensure_ascii=False)}",
    )
    try:
        # 启动异步环境初始化操作
        operation_id = await state_manager.start_environment_operation(
            user_id,
            chat_id,
            "init",
            request.config.dict(),
        )

        return {
            "success": True,
            "operation_id": operation_id,
            "message": "Environment initialization started",
            "status": "initializing",
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"Error starting init operation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cua/switch_environment")
async def switch_environment(request: InitRequest):
    """触发异步环境切换"""
    if not request.user_id:
        user_id = request.config.user_id
    else:
        user_id = request.user_id
    chat_id = request.config.chat_id
    if not user_id or not chat_id:
        raise HTTPException(
            status_code=400,
            detail="user_id and chat_id " "are required",
        )
    logger.info(
        f"start switch_environment by user_id:{user_id} , "
        f"chat_id: {chat_id}, request:{request}",
    )
    # 验证会话有效性 - 只有活跃会话才能切换环境
    await validate_user_session(user_id, chat_id, strict_mode=False)

    try:
        # 启动异步环境切换操作
        operation_id = await state_manager.start_environment_operation(
            user_id,
            chat_id,
            "switch",
            request.config.dict(),
        )

        return {
            "success": True,
            "operation_id": operation_id,
            "message": "Environment switch started",
            "status": "switching",
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error starting switch operation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cua/operation_status")
async def get_operation_status(
    user_id: str,
    chat_id: str,
    operation_id: str = None,
):
    """查询环境操作状态（用于轮询）"""
    if not user_id or not chat_id:
        logger.error("user_id and chat_id are required")
        raise HTTPException(
            status_code=400,
            detail="user_id and chat_id are required",
        )

    try:
        await state_manager.set_heartbeat(user_id, chat_id)
        status_info = await state_manager.get_environment_operation(
            user_id,
            chat_id,
            operation_id,
        )
        if status_info is None:
            logger.info(
                f"Operation not found for user_id: {user_id}, "
                f"chat_id: {chat_id}"
                f", operation_id: {operation_id}",
            )
            return {
                "success": False,
                "status": "failed",
                "error": "等待超时，请重新激活！",
                "user_id": user_id,
                "chat_id": chat_id,
                "operation_id": operation_id,
            }
        return {
            "success": True,
            **status_info,
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting operation status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cua/run")
async def run_task_stream(request: ComputerUseRequest, user_id: str = ""):
    """
    流式任务执行接口，支持序列号机制和断线续传
    如果提供sequence_number，则返回历史数据；否则执行新任务
    """
    # 从请求中提取对话ID和配置
    if not user_id:
        if request.user_id:
            user_id = request.user_id
        else:
            user_id = request.config.user_id
    chat_id = request.config.chat_id if request.config else ""
    logger.info(f"start run by user_id:{user_id}")
    if not user_id or not chat_id:
        raise HTTPException(400, "user_id and chat_id are required in config")

    # 验证用户会话有效性（非严格模式，因为心跳可能还没建立）
    await validate_user_session(user_id, chat_id, strict_mode=False)

    sequence_number = request.sequence_number

    logger.info(
        f"接收到任务请求，用户: {user_id}, 对话: {chat_id}, "
        f"序列号: {sequence_number} , request: "
        f"{json.dumps(request.model_dump(), ensure_ascii=False)}",
    )
    # 如果提供了序列号，返回历史数据（断线续传）
    if sequence_number is not None:
        return await _handle_resume_stream(user_id, chat_id, sequence_number)

    # 否则执行新任务
    if not request.input:
        raise HTTPException(400, "No input messages provided")

    return await _handle_new_stream(user_id, chat_id, request)


@app.get("/cua/stop")
async def stop_task(user_id: str, chat_id: str):
    logger.info(f"stop task by user_id:{user_id} , chat_id: {chat_id}")
    # 验证用户会话有效性（非严格模式，允许停止非活跃会话）
    await validate_user_session(user_id, chat_id, strict_mode=False)

    # 更新Redis中的停止状态
    await state_manager.stop_task(user_id, chat_id)

    # 发布停止信号到所有机器
    signal_sent = await publish_control_signal(user_id, chat_id, "stop")

    # 记录运行Agent的机器信息
    try:
        chat_state = await state_manager.get_chat_state(user_id, chat_id)
        if isinstance(chat_state, dict):
            agent_machine = chat_state.get("agent_machine_id")
        else:
            agent_machine = getattr(chat_state, "agent_machine_id", None)

        if agent_machine:
            logger.info(f"停止信号已发送到机器 {agent_machine}")
        else:
            logger.info("未找到Agent运行的机器信息，信号已广播")
    except Exception as e:
        logger.error(f"获取Agent机器信息失败: {e}")

    # 同时尝试本地停止（如果Agent在当前机器）
    try:
        composite_key = f"{user_id}:{chat_id}"
        local_agent = app.state.running_agents.get(composite_key)
        if local_agent and hasattr(local_agent, "should_stop"):
            local_agent.should_stop = True
            logger.info("本地Agent停止标志已设置")

        # 如果找到本地Agent，也将其从字典中移除
        if local_agent:
            del app.state.running_agents[composite_key]
            logger.info("本地Agent引用已清理")
    except Exception as e:
        logger.error(f"设置本地Agent停止标志失败: {e}")

    logger.info(f"task stopped, signal_sent: {signal_sent}")
    return {"success": True, "signal_sent": signal_sent}


@app.post("/cua/release")
async def release_resource(request: UserChatRequest):
    user_id = request.user_id
    chat_id = request.chat_id
    logger.info(f"release resource by user_id:{user_id} , chat_id: {chat_id}")
    # 验证用户会话有效性（非严格模式，允许释放非活跃会话资源）
    await validate_user_session(user_id, chat_id, strict_mode=False)

    await state_manager.release_resources(user_id, chat_id)
    logger.info("release success")
    return {"success": True}


@app.get("/cua/heartbeat")
async def heartbeat(user_id: str, chat_id: str):
    try:
        # 验证chat_id是否为该user_id的活跃会话
        is_valid = await state_manager.validate_user_active_chat(
            user_id,
            chat_id,
        )
        current_active_chat = await state_manager.get_user_active_chat(user_id)

        if not is_valid:
            logger.warning(
                f"Invalid chat session for user {user_id}, chat {chat_id}"
                f", active: {current_active_chat}",
            )

        # 只为有效会话更新心跳记录
        await state_manager.set_heartbeat(user_id, chat_id)

        # 同时更新heartbeats记录
        composite_key = f"{user_id}:{chat_id}"
        state_manager.heartbeats[composite_key] = time.time()

        return {"success": True, "is_active_chat": True, "status": "active"}

    except Exception as e:
        logger.error(f"Heartbeat error for {user_id}:{chat_id}: {e}")
        # 充底逻辑：即使Redis失败也要确保心跳更新
        composite_key = f"{user_id}:{chat_id}"
        state_manager.heartbeats[composite_key] = time.time()
        return {
            "success": False,
            "is_active_session": False,
            "status": "error",
            "error": str(e),
        }


@app.get("/cua/queue_status")
async def get_queue_status(user_id: str, chat_id: str, sandbox_type: str):
    try:
        if sandbox_type == "pc_wuyin":
            # 使用PC分配器并调用异步方法
            position, status = (
                await state_manager.pc_allocator.get_chat_position(
                    user_id,
                )
            )
            if (
                status == AllocationStatus.SUCCESS
                or status == AllocationStatus.CHAT_ALREADY_ALLOCATED
            ):
                queue_info = (
                    await state_manager.pc_allocator.get_queue_info_async()
                )
                return {
                    "position": position,
                    "total_waiting": queue_info["total_waiting"],
                    "queue_status": "queued",
                }
        elif sandbox_type == "phone_wuyin":
            # 使用手机分配器并调用异步方法
            s_p = state_manager.phone_allocator
            position, status = await s_p.get_chat_position(  # noqa E501
                user_id=user_id,
            )
            if (
                status == AllocationStatus.SUCCESS
                or status == AllocationStatus.CHAT_ALREADY_ALLOCATED
            ):
                queue_info = await s_p.get_queue_info_async()
                return {
                    "position": position,
                    "total_waiting": queue_info["total_waiting"],
                    "queue_status": "queued",
                }

        # 检查对话是否已分配资源
        equipment_info = await state_manager.get_equipment_info(
            user_id,
            chat_id,
        )
        if equipment_info is not None:
            return {
                "position": -1,
                "queue_status": "allocated",
            }

        # 对话不在队列且无已分配资源 - 可能是因为没有可用资源被移除或其他原因
        return {
            "position": -1,
            "queue_status": "not_queued",
        }
    except Exception as e:
        logger.error(f"Error getting queue status: {e}")
        return {
            "position": -1,
            "queue_status": "error",
            "error": str(e),
        }


# 添加代理端点来验证（魔搭场景接口） studio token
@app.get("/cua/proxy/validate-studio-token")
async def proxy_validate_studio_token(studio_token: str = Query(...)):
    """
    代理验证 studio token 的端点，避免前端 CORS 问题
    """
    try:
        # 使用异步方式发送请求到 ModelScope API
        def make_request():
            return requests.get(
                f"https://modelscope.cn/api/v1/"
                f"studios/check-token?studio_token={studio_token}",
                timeout=300,
            )

        response = await asyncio.to_thread(make_request)

        # 返回相同的响应结构
        return JSONResponse(
            content=response.json(),
            status_code=response.status_code,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cua/interrupt_wait")
async def interrupt_wait(user_id: str, chat_id: str):
    # 人工干预接口
    logger.info(f"interrupt_wait by user_id:{user_id} , chat_id: {chat_id}")
    # 验证用户会话有效性（非严格模式）
    await validate_user_session(user_id, chat_id, strict_mode=False)

    chat_state = await state_manager.get_chat_state(user_id, chat_id)

    # 检查对话是否有正在运行的任务
    if isinstance(chat_state, dict):
        is_running = chat_state.get("is_running", False)
        agent_running = chat_state.get("agent_running", False)
        agent_machine = chat_state.get("agent_machine_id")
    else:
        is_running = getattr(chat_state, "is_running", False)
        agent_running = getattr(chat_state, "agent_running", False)
        agent_machine = getattr(chat_state, "agent_machine_id", None)

    # 没有正在运行的任务
    if not is_running or not agent_running:
        raise HTTPException(status_code=400, detail="No running task")

    # 发布中断等待信号到所有机器
    signal_sent = await publish_control_signal(
        user_id,
        chat_id,
        "interrupt_wait",
    )

    # 记录运行Agent的机器信息
    if agent_machine:
        logger.info(f"中断等待信号已发送到机器 {agent_machine}")
    else:
        logger.info("未找到Agent运行的机器信息，信号已广播")

    # 同时尝试本地中断（如果Agent在当前机器）
    try:
        composite_key = f"{user_id}:{chat_id}"
        local_agent = app.state.running_agents.get(composite_key)
        if local_agent and hasattr(local_agent, "interrupt_wait"):
            local_agent.interrupt_wait()
            logger.info("本地Agent中断等待已调用")
        elif local_agent is None:
            logger.info("本地未找到Agent实例")
        else:
            logger.warning("本地Agent不支持interrupt_wait方法")
    except Exception as e:
        logger.error(f"调用本地Agent中断等待失败: {e}")

    # 向前端发送状态更新
    await state_manager.update_status(
        user_id,
        chat_id,
        {
            "status": "running",
            "type": "SYSTEM",
            "message": "Stop-wait request received",
        },
    )
    return {"success": True, "signal_sent": signal_sent}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("backend:app", host="0.0.0.0", port=8002, reload=True)
