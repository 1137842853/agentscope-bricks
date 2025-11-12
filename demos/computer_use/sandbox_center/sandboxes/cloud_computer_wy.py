# -*- coding: utf-8 -*-
import os
import aiohttp
import time
import asyncio
import threading
import base64
from typing import List, Tuple, Any, Callable, Optional
from pydantic import BaseModel
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ecd20200930.client import Client as ecd20200930Client
from alibabacloud_ecd20200930 import models as ecd_20200930_models
from alibabacloud_appstream_center20210218 import (
    models as appstream_center_20210218_models,
)
from alibabacloud_appstream_center20210218.client import (
    Client as appstream_center20210218Client,
)
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient
from sandbox_center.utils.oss_client import OSSClient
from sandbox_center.sandboxes.sandbox_base import (
    SandboxBase,
    OperationStatus,
)
from agentscope_bricks.utils.logger_util import logger

execute_wait_time_: int = 3


class ClientPool:
    """客户端池管理器 - 单例模式管理共享客户端实例"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._ecd_client = None
            self._oss_client = None
            self._app_stream_client = None
            self._instance_managers = {}  # 按desktop_id缓存EcdInstanceManager
            # 使用不同的锁来避免死锁
            self._ecd_lock = threading.Lock()
            self._oss_lock = threading.Lock()
            self._app_stream_lock = threading.Lock()
            self._instance_manager_lock = threading.Lock()
            self._initialized = True

    def get_ecd_client(self) -> "EcdClient":
        """获取共享的EcdClient实例"""
        if self._ecd_client is None:
            with self._ecd_lock:
                if self._ecd_client is None:
                    self._ecd_client = EcdClient()
        return self._ecd_client

    def get_oss_client(self) -> OSSClient:
        """获取共享的OSSClient实例"""
        if self._oss_client is None:
            with self._oss_lock:
                if self._oss_client is None:
                    bucket_name = os.environ.get("EDS_OSS_BUCKET_NAME")
                    endpoint = os.environ.get("EDS_OSS_ENDPOINT")
                    self._oss_client = OSSClient(bucket_name, endpoint)
        return self._oss_client

    def get_app_stream_client(
        self,
        desktop_id: str = None,
    ) -> "AppStreamClient":
        """获取AppStreamClient实例，每次调用都创建新的实例（非共享模式）"""
        # 每次都创建新的AppStreamClient实例，不使用缓存
        return AppStreamClient()

    def get_instance_manager(self, desktop_id: str) -> "EcdInstanceManager":
        """获取指定desktop_id的EcdInstanceManager实例"""
        # 先检查是否已存在，避免不必要的锁竞争
        if desktop_id in self._instance_managers:
            return self._instance_managers[desktop_id]

        # 在锁外预先获取客户端，避免死锁
        ecd_client = self.get_ecd_client()
        oss_client = self.get_oss_client()
        app_stream_client = self.get_app_stream_client(desktop_id)

        # 使用专门的锁管理实例管理器
        with self._instance_manager_lock:
            # 再次检查，防止在等待锁的过程中已经被其他线程创建
            if desktop_id not in self._instance_managers:
                # 创建新的实例管理器，并传入共享的客户端
                manager = EcdInstanceManager(desktop_id)
                manager.ecd_client = ecd_client
                manager.oss_client = oss_client
                manager.app_stream_client = app_stream_client
                self._instance_managers[desktop_id] = manager
        return self._instance_managers[desktop_id]


class CloudComputer(SandboxBase):
    def __init__(self, desktop_id: str = "") -> None:
        # 📝 直接使用传入的 desktop_id，不再使用本地缓存
        if not desktop_id:
            desktop_id = os.environ.get("ECD_DESKTOP_ID")

        if not desktop_id:
            raise Exception(
                "desktop_id is required for CloudComputer initialization",
            )

        self.instance_manager = self.get_instance_manager(desktop_id)
        if self.instance_manager is None:
            raise Exception(
                "get instance manager failed, could "
                "not create EcdInstanceManager",
            )

    async def initialize(self) -> None:
        """异步初始化方法"""
        await self.instance_manager.init_resources()

    def execute_wait_time_set(self, execute_wait_time: int = 5) -> str:
        execute_wait_time_ = execute_wait_time
        logger.info(f"set slot time to {execute_wait_time_}")
        return "The slot time has been set."

    # 抽象方法重写

    async def run_command(
        self,
        command: str,
        background: bool = False,
        timeout: int = 5,
        ope_type: str = None,
    ) -> str:
        status, res = await self.instance_manager.run_command_power_shell(
            command,
            3,
            timeout,
        )
        return f"command has benn done {status},res: {res}"

    async def press_key(
        self,
        key: str = None,
        key_combination: List[str] = None,
        ope_type: str = None,
    ) -> str:
        await self.instance_manager.press_key(key)
        return "The key has been pressed."

    async def long_press(self, x: int, y: int, press_time: str) -> str:
        return OperationStatus.DEVICE_UN_SUPPORTED_OPERATION.value

    async def type_text(self, text: str, ope_type: str = None) -> str:
        return OperationStatus.DEVICE_UN_SUPPORTED.value

    def click_element(
        self,
        query: str,
        click_command: Callable,
        action_name: str = "click",
    ) -> str:
        return OperationStatus.DEVICE_UN_SUPPORTED.value

    async def click(
        self,
        x: int = 0,
        y: int = 0,
        count: int = 1,
        query: str = "",
        action_name: str = "click",
        ope_type: str = None,
        x2: int = 0,
        y2: int = 0,
        width: int = 0,
        height: int = 0,
    ) -> str:
        await self.instance_manager.tap(x, y, count)
        return f"The mouse has clicked {count} times at ({x}, {y})."

    async def right_click(
        self,
        x: int,
        y: int,
        count: int = 1,
        ope_type: str = None,
    ) -> str:
        await self.instance_manager.right_tap(x, y, count)
        return f"The mouse has right clicked at ({x}, {y})."

    async def click_and_type(
        self,
        x: int,
        y: int,
        text: str,
        ope_type: str = None,
    ) -> str:
        await self.instance_manager.tap_type_enter(x, y, text)
        return f"The mouse has clicked and typed the text at ({x}, {y})."

    async def append_text(
        self,
        x: int,
        y: int,
        text: str,
        ope_type: str = None,
    ) -> str:
        await self.instance_manager.append(x, y, text)
        return f"Append the content at ({x}, {y}) with {text} and press Enter"

    async def launch_app(self, app: str, ope_type: str = None) -> str:
        await self.instance_manager.open_app(app)
        return f"The application {app} has been launched."

    async def go_home(self, action: str) -> str:
        await self.instance_manager.home()
        return f"This {action} has been done"

    async def slide(self, x1: int, y1: int, x2: int, y2: int) -> str:
        return OperationStatus.DEVICE_UN_SUPPORTED.value

    async def back(self, action: str) -> str:
        return OperationStatus.DEVICE_UN_SUPPORTED.value

    async def menu(self, action: str) -> str:
        return OperationStatus.DEVICE_UN_SUPPORTED.value

    async def enter(self, action: str) -> str:
        return OperationStatus.DEVICE_UN_SUPPORTED.value

    async def kill_front_app(self, action: str) -> str:
        return OperationStatus.DEVICE_UN_SUPPORTED.value

    # 抽象方法重写结束

    def get_instance_manager(self, desktop_id: str) -> Any:
        retry = 3
        while retry > 0:
            try:
                # 使用ClientPool获取实例管理器，避免重复创建客户端连接
                client_pool = ClientPool()
                manager = client_pool.get_instance_manager(desktop_id)
                return manager
            except Exception as e:
                retry -= 1
                logger.warning(
                    f"get manager error, retrying: remain {retry}, {e}",
                )
                continue
        return None

    def upload_local_file_oss(self, file: str, file_name: str) -> str:
        return self.instance_manager.local_file_upload_oss(file, file_name)

    async def upload_file_and_sign(self, filepath: str, file_name: str) -> str:
        return await self.instance_manager.in_upload_file_and_sign(
            filepath,
            file_name,
        )

    async def get_screenshot_base64_save_local(
        self,
        local_file_name: str,
        local_save_path: str,
        max_retry: int = 5,
    ) -> str:
        for _ in range(max_retry):
            screen_base64 = await self.instance_manager.get_screenshot(
                local_file_name,
                local_save_path,
            )
            if screen_base64:
                return screen_base64
        return "Error"

    async def get_screenshot_oss_save_local(
        self,
        local_file_name: str,
        local_save_path: str,
        max_retry: int = 5,
    ) -> str:
        for _ in range(max_retry):
            screen_oss_url = (
                await self.instance_manager.get_screenshot_oss_url(
                    local_file_name,
                    local_save_path,
                )
            )
            if screen_oss_url:
                return screen_oss_url
        return "Error"

    async def operate(
        self,
        operation: dict,
        stop_flag: bool,
    ) -> bool:  # 实际坐标
        logger.debug(f"操作参数: {operation}")
        try:
            action_type = operation["action_type"]
            action_parameter = None
            if action_type == "subtask_stop":
                logger.info("subtask stop")
                stop_flag = False
            elif action_type == "stop":
                logger.info("stop")
                stop_flag = True
            else:
                action_parameter = operation["action_parameter"]

            if action_type == "open app":
                if action_parameter is not None:
                    name = action_parameter["name"]
                    if name == "File Explorer":
                        name = "⽂件资源管理器"
                    await self.instance_manager.open_app(name)
            elif action_type == "click":
                if action_parameter is not None:
                    x = action_parameter["position"][0]
                    y = action_parameter["position"][1]
                    count = action_parameter["count"]
                    await self.instance_manager.tap(x, y, count=count)
            elif action_type == "right click":
                if action_parameter is not None:
                    x = action_parameter["position"][0]
                    y = action_parameter["position"][1]
                    count = action_parameter["count"]
                    await self.instance_manager.right_tap(x, y, count=count)
            elif action_type == "hotkey":
                if action_parameter is not None:
                    key_list = action_parameter["key_list"]
                    await self.instance_manager.hotkey(key_list)
            elif action_type == "presskey":
                if action_parameter is not None:
                    key = action_parameter["key"]
                    await self.instance_manager.press_key(key)
            elif action_type == "click_type":
                if action_parameter is not None:
                    x = action_parameter["position"][0]
                    y = action_parameter["position"][1]
                    text = action_parameter["text"]
                    await self.instance_manager.tap_type_enter(x, y, text)
            elif action_type == "drag":
                if action_parameter is not None:
                    x1 = action_parameter["position1"][0]
                    y1 = action_parameter["position1"][1]
                    x2 = action_parameter["position2"][0]
                    y2 = action_parameter["position2"][1]
                    await self.instance_manager.drag(x1, y1, x2, y2)
            elif action_type == "replace":
                if action_parameter is not None:
                    x = action_parameter["position"][0]
                    y = action_parameter["position"][1]
                    text = action_parameter["text"]
                    await self.instance_manager.replace(x, y, text)
            elif action_type == "append":
                if action_parameter is not None:
                    x = action_parameter["position"][0]
                    y = action_parameter["position"][1]
                    text = action_parameter["text"]
                    await self.instance_manager.append(x, y, text)
            elif action_type == "tell":
                if action_parameter is not None:
                    answer_dict = action_parameter["answer"]
                    logger.debug(f"告知应答: {answer_dict}")
            elif action_type == "mouse_move":  # New
                if action_parameter is not None:
                    x = action_parameter["position"][0]
                    y = action_parameter["position"][1]
                    await self.instance_manager.mouse_move(x, y)
            elif action_type == "middle_click":  # New
                if action_parameter is not None:
                    x = action_parameter["position"][0]
                    y = action_parameter["position"][1]
                    await self.instance_manager.middle_click(x, y)
            elif action_type == "type_with_clear_enter":  # New
                if action_parameter is not None:
                    clear = action_parameter["clear"]
                    enter = action_parameter["enter"]
                    text = action_parameter["text"]
                    await self.instance_manager.type_with_clear_enter(
                        text,
                        clear,
                        enter,
                    )
            elif action_type == "type_with_clear_enter_pos":  # New
                if action_parameter is not None:
                    clear = action_parameter["clear"]
                    enter = action_parameter["enter"]
                    text = action_parameter["text"]
                    x = action_parameter["position"][0]
                    y = action_parameter["position"][1]
                    await self.instance_manager.type_with_clear_enter_pos(
                        text,
                        x,
                        y,
                        clear,
                        enter,
                    )
            elif action_type == "scroll":
                if (
                    action_parameter is not None
                    and "position" in action_parameter
                ):  # -E
                    x = action_parameter["position"][0]
                    y = action_parameter["position"][1]
                    pixels = action_parameter["pixels"]
                    await self.instance_manager.scroll_pos(x, y, pixels)
            else:  # e2e
                if action_parameter is not None:
                    pixels = action_parameter["pixels"]
                    await self.instance_manager.scroll(pixels)
            return stop_flag

        except Exception as e:
            logger.error(f"操作执行失败: {e}")
            return False


class EcdDeviceInfo(BaseModel):
    # 云电脑设备信息查询字段返回类
    connection_status: str = (None,)
    desktop_id: str = (None,)
    desktop_status: str = (None,)
    start_time: str = (None,)


class EcdClient:

    def __init__(self) -> None:
        config = open_api_models.Config(
            access_key_id=os.environ.get("ECD_ALIBABA_CLOUD_ACCESS_KEY_ID"),
            # 您的AccessKey Secret,
            access_key_secret=os.environ.get(
                "ECD_ALIBABA_CLOUD_ACCESS_KEY_SECRET",
            ),
        )
        # Endpoint 请参考 https://api.aliyun.com/product/eds-aic
        config.endpoint = os.environ.get("ECD_ALIBABA_CLOUD_ENDPOINT")
        self.__client__ = ecd20200930Client(config)

    def execute_command(
        self,
        desktop_ids: List[str],
        command: str,
        timeout: int = 60,
    ) -> Tuple[str, str]:
        # 执行命令
        run_command_request = ecd_20200930_models.RunCommandRequest(
            desktop_id=desktop_ids,
            command_content=command,
            type="RunPowerShellScript",
            end_user_id=os.environ.get("ECD_USERNAME"),
            content_encoding="PlainText",
            timeout=timeout,
        )
        runtime = util_models.RuntimeOptions()
        try:
            rsp = self.__client__.run_command_with_options(
                run_command_request,
                runtime,
            )

            assert rsp.status_code == 200
            invoke_id = rsp.body.invoke_id
            request_id = rsp.body.request_id
            # logging.info(invoke_id, request_id)
            return invoke_id, request_id
        except Exception as error:
            logger.error(f"{desktop_ids} excute command failed:{error}")
            return "", ""

    def query_execute_state(
        self,
        desktop_ids: List[str],
        message_id: str,
    ) -> Any:
        # 查询命令执行结果
        describe_invocations_request = (
            ecd_20200930_models.DescribeInvocationsRequest(
                desktop_ids=desktop_ids,
                invoke_id=message_id,
                end_user_id=os.environ.get("ECD_USERNAME"),
                command_type="RunPowerShellScript",
                content_encoding="PlainText",
                include_output=True,
            )
        )
        runtime = util_models.RuntimeOptions()
        try:
            rsp = self.__client__.describe_invocations_with_options(
                describe_invocations_request,
                runtime,
            )
            # print(rsp.body)
            return rsp.body
        except Exception as error:
            UtilClient.assert_as_string(error)
            logger.error(f"{desktop_ids} query message failed:{error}")
        return None

    def run_command_with_wait(
        self,
        desktop_id: str,
        command: str,
        slot_time: float = None,
        timeout: int = 60,
    ) -> Tuple[str, str]:
        execute_id, request_id = self.execute_command(
            [desktop_id],
            command,
            timeout=timeout,
        )
        start_time = time.time()
        if not slot_time:
            if (
                "execute_wait_time_" in globals()
                and execute_wait_time_ is not None
            ):
                slot_time = execute_wait_time_
            else:
                slot_time = 3  # 默认值
        slot_time = max(0.5, slot_time)
        timeout = slot_time + timeout
        if execute_id:
            while timeout > 0:
                logger.info("start wait execution")
                time.sleep(slot_time)
                logger.info("execution end")
                msgs = self.query_execute_state(
                    [desktop_id],
                    execute_id,
                )
                for msg in msgs.invocations:
                    if msg.invocation_status in [
                        "Success",
                        "Failed",
                        "Timeout",
                    ]:
                        logger.info(
                            f"command cost time: {time.time() - start_time}",
                        )
                        return (
                            msg.invocation_status == "Success",
                            msg.invoke_desktops[0].output,
                        )
                timeout -= slot_time
        raise Exception("screenshot timeout")

    async def run_command_with_wait_async(
        self,
        desktop_id: str,
        command: str,
        slot_time: float = None,
        timeout: int = 60,
    ) -> Tuple[bool, str]:
        execute_id, request_id = self.execute_command(
            [desktop_id],
            command,
            timeout=timeout,
        )
        start_time = time.time()
        if not slot_time:
            if (
                "execute_wait_time_" in globals()
                and execute_wait_time_ is not None
            ):
                slot_time = execute_wait_time_
            else:
                slot_time = 3  # 默认值
        slot_time = max(0.5, slot_time)
        timeout = slot_time + timeout
        if execute_id:
            while timeout > 0:
                logger.info("start wait execution")
                await asyncio.sleep(slot_time)  # 使用 asyncio.sleep
                logger.info("execution end")
                msgs = self.query_execute_state(
                    [desktop_id],
                    execute_id,
                )
                if msgs is None:
                    raise Exception("query execute state failed")

                for msg in msgs.invocations:
                    if msg.invocation_status in [
                        "Success",
                        "Failed",
                        "Timeout",
                    ]:
                        logger.info(
                            f"command cost time: {time.time() - start_time}",
                        )
                        return (
                            msg.invocation_status == "Success",
                            (
                                msg.invoke_desktops[0].output
                                if msg.invoke_desktops
                                else ""
                            ),
                        )
                timeout -= slot_time
        raise Exception("command timeout")

    def search_desktop_info(
        self,
        desktop_ids: List[str],
    ) -> List[EcdDeviceInfo]:
        describe_desktop_info_request = (
            ecd_20200930_models.DescribeDesktopInfoRequest(
                region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
                desktop_id=desktop_ids,
            )
        )

        runtime = util_models.RuntimeOptions()
        try:
            rsp = self.__client__.describe_desktop_info_with_options(
                describe_desktop_info_request,
                runtime,
            )
            devices_info = [
                EcdDeviceInfo(**inst.__dict__) for inst in rsp.body.desktops
            ]
            return devices_info
        except Exception as error:
            logger.error(f"search wuying desktop failed:{error}")
            return []

    def start_desktops(self, desktop_ids: List[str]) -> int:
        start_desktops_request = ecd_20200930_models.StartDesktopsRequest(
            region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
            desktop_id=desktop_ids,
        )

        runtime = util_models.RuntimeOptions()
        try:
            e_c = self.__client__
            rsp = e_c.start_desktops_with_options(
                start_desktops_request,
                runtime,
            )
            logger.info(
                f"[{desktop_ids}]: start instance ask api success,"
                f" and wait finish",
            )
            return rsp.status_code
        except Exception as error:
            logger.error(f"start_desktops failed:{error}")
            return 400

    async def start_desktops_async(self, desktop_ids: List[str]) -> int:
        start_desktops_request = ecd_20200930_models.StartDesktopsRequest(
            region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
            desktop_id=desktop_ids,
        )

        runtime = util_models.RuntimeOptions()
        try:
            e_c = self.__client__
            method = e_c.start_desktops_with_options_async
            rsp = await method(
                start_desktops_request,
                runtime,
            )
            logger.info(
                f"[{desktop_ids}]: start instance ask api success,"
                f" and wait finish",
            )
            return rsp.status_code
        except Exception as error:
            logger.error(f"start_desktops failed:{error}")
            return 400

    def wakeup_desktops(self, desktop_ids: List[str]) -> int:
        wakeup_desktops_request = ecd_20200930_models.WakeupDesktopsRequest(
            region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
            desktop_id=desktop_ids,
        )
        runtime = util_models.RuntimeOptions()
        try:
            e_c = self.__client__
            rsp = e_c.wakeup_desktops_with_options(
                wakeup_desktops_request,
                runtime,
            )
            logger.info(
                f"[{desktop_ids}]: wakeup instance ask api success,"
                f" and wait finish",
            )
            return rsp.status_code
        except Exception as error:
            logger.error(f"wakeup_desktops failed:{error}")
            return 400

    def hibernate_desktops(self, desktop_ids: List[str]) -> int:
        hibernate_desktops_request = (
            ecd_20200930_models.HibernateDesktopsRequest(
                region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
                desktop_id=desktop_ids,
            )
        )
        runtime = util_models.RuntimeOptions()
        try:
            e_c = self.__client__
            rsp = e_c.hibernate_desktops_with_options(
                hibernate_desktops_request,
                runtime,
            )
            logger.info(
                f"[{desktop_ids}]: hibernate instance ask api success,"
                f" and wait finish",
            )
            return rsp.status_code
        except Exception as error:
            logger.error(f"hibernate_desktops failed:{error}")
            return 400

    async def wakeup_desktops_async(self, desktop_ids: List[str]) -> int:
        wakeup_desktops_request = ecd_20200930_models.WakeupDesktopsRequest(
            region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
            desktop_id=desktop_ids,
        )
        runtime = util_models.RuntimeOptions()
        try:
            e_c = self.__client__
            method = e_c.wakeup_desktops_with_options_async
            rsp = await method(
                wakeup_desktops_request,
                runtime,
            )
            logger.info(
                f"[{desktop_ids}]: wakeup instance ask api success,"
                f" and wait finish",
            )
            return rsp.status_code
        except Exception as error:
            logger.error(f"wakeup_desktops failed:{error}")
            return 400

    async def hibernate_desktops_async(self, desktop_ids: List[str]) -> int:
        hibernate_desktops_request = (
            ecd_20200930_models.HibernateDesktopsRequest(
                region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
                desktop_id=desktop_ids,
            )
        )
        runtime = util_models.RuntimeOptions()
        try:
            e_c = self.__client__
            method = e_c.hibernate_desktops_with_options_async
            rsp = await method(
                hibernate_desktops_request,
                runtime,
            )
            logger.info(
                f"[{desktop_ids}]: wakeup instance ask api success,"
                f" and wait finish",
            )
            return rsp.status_code
        except Exception as error:
            logger.error(f"hibernate_desktops failed:{error}")
            return 400

    async def restart_equipment(self, desktop_id: str) -> int:
        reboot_desktops_request = ecd_20200930_models.RebootDesktopsRequest(
            region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
            desktop_id=[desktop_id],
        )
        runtime = util_models.RuntimeOptions()
        try:
            rsp = await self.__client__.reboot_desktops_with_options_async(
                reboot_desktops_request,
                runtime,
            )
            return rsp.status_code
        except Exception as error:
            logger.error(f"restart equipment failed:{error}")
        return 400

    def stop_desktops(self, desktop_ids: List[str]) -> int:
        stop_desktops_request = ecd_20200930_models.StopDesktopsRequest(
            region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
            desktop_id=desktop_ids,
        )

        runtime = util_models.RuntimeOptions()
        try:
            rsp = self.__client__.stop_desktops_with_options(
                stop_desktops_request,
                runtime,
            )
            return rsp.status_code
        except Exception as error:
            logger.error(f"stop_desktops failed:{error}")
        return 400

    async def stop_desktops_async(self, desktop_ids: List[str]) -> int:
        stop_desktops_request = ecd_20200930_models.StopDesktopsRequest(
            region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
            desktop_id=desktop_ids,
        )

        runtime = util_models.RuntimeOptions()
        try:
            e_c = self.__client__
            method = e_c.stop_desktops_with_options_async
            rsp = await method(
                stop_desktops_request,
                runtime,
            )
            logger.info(
                f"[{desktop_ids}]: wakeup instance ask api success,"
                f" and wait finish",
            )
            return rsp.status_code
        except Exception as error:
            logger.error(f"stop_desktops failed:{error}")
        return 400

    async def rebuild_equipment_image(
        self,
        desktop_id: str,
        image_id: str,
    ) -> int:
        rebuild_request = ecd_20200930_models.RebuildDesktopsRequest(
            region_id=os.environ.get("ECD_ALIBABA_CLOUD_REGION_ID"),
            image_id=image_id,
            desktop_id=[
                desktop_id,
            ],
        )
        runtime = util_models.RuntimeOptions()
        try:
            rsp = await self.__client__.rebuild_desktops_with_options_async(
                rebuild_request,
                runtime,
            )
            return rsp.status_code
        except Exception as error:
            logger.error(f"rebuild equipment failed:{error}")
        return 400


class EcdInstanceManager:
    def __init__(self, desktop_id: str = None) -> None:
        self.desktop_id = desktop_id
        self.ctrl_key = None
        self.ratio = None
        self.oss_sk = None
        self.oss_ak = None
        self.endpoint = None
        self.oss_client = None
        self.ecd_client = None
        self._initialized = False
        self._init_error = None
        self.app_stream_client = None
        self.auth_code = None

    async def init_resources(self) -> bool:
        if self._initialized:
            # 获取新的auth_code
            self.auth_code = await self.app_stream_client.search_auth_code()
            return True
        try:
            # 如果没有预设的客户端（通过ClientPool设置），则创建新的
            if self.ecd_client is None:
                self.ecd_client = EcdClient()
            if self.app_stream_client is None:
                # 每次都创建新的AppStreamClient实例（非共享模式）
                self.app_stream_client = AppStreamClient()
            if self.oss_client is None:
                bucket_name = os.environ.get("EDS_OSS_BUCKET_NAME")
                endpoint = os.environ.get("EDS_OSS_ENDPOINT")
                self.oss_client = OSSClient(bucket_name, endpoint)

            # 获取auth_code
            self.auth_code = await self.app_stream_client.search_auth_code()

            # 📝 验证 desktop_id 是否有效（可选）
            if self.desktop_id and self.ecd_client:
                # 验证设备是否存在和可用
                desktop_info = self.ecd_client.search_desktop_info(
                    [self.desktop_id],
                )
                if not desktop_info:
                    raise Exception(
                        f"Desktop {self.desktop_id} not found "
                        f"or not accessible",
                    )

            # 设置OSS endpoint
            self.endpoint = os.environ.get("EDS_OSS_ENDPOINT")

            # 🔑 配置参数
            self.oss_ak = os.environ.get("EDS_OSS_ACCESS_KEY_ID")
            self.oss_sk = os.environ.get("EDS_OSS_ACCESS_KEY_SECRET")
            self.ratio = 1
            self.ctrl_key = "ctrl"

            self._initialized = True
            return True
        except Exception as e:
            self._init_error = e
            logger.error(f"Initialization failed: {e}")
            return False

    async def get_screenshot(
        self,
        local_file_name: str,
        local_save_path: str,
    ) -> str:
        # local_file_name = f"{uuid.uuid4().hex}__screenshot"
        logger.info("开始截图")
        save_path = f"C:/file/{local_file_name}"
        file_save_path = f"{local_file_name}.png"
        file_local_save_path = f"{save_path}.png"
        retry = 2
        while retry > 0:
            try:
                # 截图
                # 获取oss预签名url上传地址
                oss_signed_url = await self.oss_client.async_get_signal_url(
                    f"{file_save_path}",
                )
                status, file_oss = await self.get_screenshot_oss(
                    f"{save_path}.png",
                    file_local_save_path,
                    oss_signed_url,
                )
                logger.debug(f"文件输出: {file_oss}")
                if "Traceback" in file_oss:
                    return ""
                base64_image = ""
                file_oss_down = await self.oss_client.async_get_download_url(
                    f"{file_save_path}",
                )
                if status and file_oss:
                    base64_image = await download_oss_image_and_save(
                        file_oss_down,
                        local_save_path,
                    )
                    if base64_image:
                        logger.info("成功获取Base64图片数据")
                        return base64_image

                return f"data:image/png;base64,{base64_image}"

            except Exception as e:
                retry -= 1
                logger.warning(f"截图失败，重试中... {retry}次剩余，错误: {e}")
                await asyncio.sleep(2)

        return ""

    async def get_screenshot_oss_url(
        self,
        local_file_name: str,
        local_save_path: str,
    ) -> str:
        # local_file_name = f"{uuid.uuid4().hex}__screenshot"
        save_path = f"C:/file/{local_file_name}"
        file_save_path = f"{local_file_name}.png"
        file_local_save_path = f"{save_path}.png"
        retry = 3
        while retry > 0:
            try:
                # 截图
                # 获取oss预签名url上传地址
                oss_signed_url = await self.oss_client.async_get_signal_url(
                    f"{file_save_path}",
                )
                status, file_oss = await self.get_screenshot_oss(
                    f"{save_path}.png",
                    file_local_save_path,
                    oss_signed_url,
                )
                if "Traceback" in file_oss:
                    return ""
                file_oss_down = await self.oss_client.async_get_download_url(
                    f"{file_save_path}",
                )
                if status and file_oss:
                    await download_oss_image_and_save(
                        file_oss_down,
                        local_save_path,
                    )
                    if file_oss_down:
                        logger.info("成功获取图片数据")
                        return file_oss_down
            except Exception as e:
                retry -= 1
                logger.warning(f"截图失败，重试中... {retry}次剩余，错误: {e}")
                await asyncio.sleep(2)

        return ""

    async def get_screenshot_oss_down(self, local_file_name: str) -> str:
        # local_file_name = f"{uuid.uuid4().hex}__screenshot"
        save_path = f"C:/file/{local_file_name}"
        file_save_path = f"{local_file_name}.png"
        file_local_save_path = f"{save_path}.png"
        retry = 3
        while retry > 0:
            try:
                # 截图
                # 获取oss预签名url上传地址
                oss_signed_url = await self.oss_client.async_get_signal_url(
                    f"{file_save_path}",
                )
                await self.get_screenshot_oss(
                    f"{save_path}.png",
                    file_local_save_path,
                    oss_signed_url,
                )
                file_oss_down = await self.oss_client.async_get_download_url(
                    f"{file_save_path}",
                )
                return file_oss_down

            except Exception as e:
                retry -= 1
                logger.warning(f"截图失败，重试中... {retry}次剩余，错误: {e}")
                await asyncio.sleep(2)

        return ""

    def local_file_upload_oss(self, file: str, file_name: str) -> str:
        return self.oss_client.upload_local_and_sign(file, file_name)

    async def in_upload_file_and_sign(
        self,
        filepath: str,
        file_name: str,
    ) -> str:
        return await self.oss_client.async_oss_upload_file_and_sign(
            filepath,
            file_name,
        )

    async def run_command_power_shell(
        self,
        command: str,
        slot_time: float = None,
        timeout: int = 30,
    ) -> Tuple[str, str]:

        return await self.ecd_client.run_command_with_wait_async(
            self.desktop_id,
            command,
            slot_time,
            timeout,
        )

    async def get_screenshot_base64(
        self,
        screenshot_file: str,
    ) -> Tuple[str, str]:
        script = f"""
import pyautogui
import os
import base64
screenshot_file = r'{screenshot_file}'
if os.path.exists(screenshot_file):
    os.remove(screenshot_file)
screenshot = pyautogui.screenshot()
screenshot.save(screenshot_file)
with open(screenshot_file, 'rb') as img_file:
    image_data = img_file.read()
encoded_bytes = base64.b64encode(image_data).decode('utf-8')
print(encoded_bytes)
os.remove(screenshot_file)
        """.format(
            screenshot_file=screenshot_file,
        )

        # 转义双引号
        # escaped_script = script.replace('"', '""')

        # 构建 Python 命令并进行 Base64 编码（使用 utf-16le）
        full_python_command = f'\npython -c "{script}"'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def get_screenshot_oss(
        self,
        screenshot_file: str,
        file_save_path: str,
        oss_signal_url: str,
    ) -> Tuple[str, str]:

        script = f"""
import pyautogui
import os
import base64
import requests
oss_signal_url = r'{oss_signal_url}'
file_save_path = r'{file_save_path}'
def upload_file(signed_url, file_path):
    try:
        with open(file_path, 'rb') as file:
            response = requests.put(signed_url, data=file)
        print(response.status_code)
    except Exception as e:
        print(e)
screenshot_file = r'{screenshot_file}'
if os.path.exists(screenshot_file):
    os.remove(screenshot_file)
screenshot = pyautogui.screenshot()
screenshot.save(screenshot_file)
upload_file(oss_signal_url, file_save_path)
print(oss_signal_url)
os.remove(screenshot_file)
    """.format(
            screenshot_file=screenshot_file,
            oss_signal_url=oss_signal_url,
            file_save_path=file_save_path,
        )

        # 转义双引号
        # escaped_script = script.replace('"', '""')

        # 构建 Python 命令并进行 Base64 编码（使用 utf-16le）
        full_python_command = f'\npython -c "{script}"'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def open_app(self, name: str) -> Tuple[str, str]:
        script = f"""
import pyautogui
import pyperclip
import time
import re
pyautogui.FAILSAFE = False

# 定义快捷键
ctrl_key = '{self.ctrl_key}'

def contains_chinese(text):
    return bool(re.search(r'[\u4e00-\u9fff]', text))

name = '{name}'
if 'Outlook' in name:
    name = name.replace('Outlook', 'Outlook new')

print(f'Action: open {name}')

# 打开 Windows 搜索栏
pyautogui.press('win')  # 按下 Win 键
time.sleep(0.5)
pyperclip.copy(name)
pyautogui.hotkey(ctrl_key, 'v')  # 使用 Ctrl+V 粘贴
# 回车确认
time.sleep(1)
pyautogui.press('enter')
"""
        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def home(self) -> Tuple[str, str]:
        # 显示桌面
        script = """
import pyautogui
pyautogui.FAILSAFE = False
key1 = 'win'
key2 = 'd'
pyautogui.keyDown(key1)
pyautogui.keyDown(key2)
pyautogui.keyUp(key2)
pyautogui.keyUp(key1)
        """
        full_python_command = f'\npython -c "{script}"'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def tap(self, x: int, y: int, count: int = 1) -> Tuple[str, str]:
        script = f"""
import pyautogui
from pynput.mouse import Button, Controller
pyautogui.FAILSAFE = False
ratio = {self.ratio}
x = {x}
y = {y}
count = {count}
x, y = x//ratio, y//ratio
print('Action: click (%d, %d) %d times' % (x, y, count))
mouse = Controller()
pyautogui.moveTo(x,y)
mouse.click(Button.left, count=count)
"""
        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def right_tap(
        self,
        x: int,
        y: int,
        count: int = 1,
    ) -> Tuple[str, str]:
        script = f"""
import pyautogui
from pynput.mouse import Button, Controller
pyautogui.FAILSAFE = False
ratio = {self.ratio}
x = {x}
y = {y}
count = {count}
x, y = x//ratio, y//ratio
print('Action: right click (%d, %d) %d times' % (x, y, count))
pyautogui.rightClick(x, y)
"""
        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def shortcut(self, key1: str, key2: str) -> Tuple[str, str]:
        script = f"""
import pyautogui
pyautogui.FAILSAFE = False
key1 = '{key1}'
key2 = '{key2}'
ctrl_key = '{self.ctrl_key}'
if key1 == 'command' or key1 == 'ctrl':
    key1 = ctrl_key
print('Action: shortcut %s + %s' % (key1, key2))
pyautogui.keyDown(key1)
pyautogui.keyDown(key2)
pyautogui.keyUp(key2)
pyautogui.keyUp(key1)
"""

        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def hotkey(self, key_list: List[str]) -> Tuple[str, str]:
        """
        远程执行组合键操作（例如 ['ctrl', 'c']、['alt', 'f4'] 等）
        :param key_list: 组合键列表，如 ['ctrl', 'a'], ['alt', 'f4']
        """
        script = f"""
import pyautogui
pyautogui.FAILSAFE = False
pyautogui.hotkey('{key_list[0]}', '{key_list[1]}')
"""

        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def press_key(self, key: str) -> Tuple[str, str]:
        script = f"""
import pyautogui
pyautogui.FAILSAFE = False
pyautogui.press('{key}')
"""

        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def tap_type_enter(
        self,
        x: int,
        y: int,
        text: str,
    ) -> Tuple[str, str]:
        script = f"""
import pyautogui
import pyperclip
import time
pyautogui.FAILSAFE = False
ratio = {self.ratio}
ctrl_key = '{self.ctrl_key}'
x = {x}
y = {y}
text = '{text}'
x, y = x//ratio, y//ratio
print('Action: click (%d, %d), enter %s and press Enter' % (x, y, text))
pyautogui.click(x=x, y=y)
time.sleep(0.5)
pyperclip.copy(text)
pyautogui.keyDown(ctrl_key)
pyautogui.keyDown('v')
pyautogui.keyUp('v')
pyautogui.keyUp(ctrl_key)
time.sleep(0.5)
pyautogui.press('enter')
"""

        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def drag(
        self,
        x1: int,
        y1: int,
        x2: int,
        y2: int,
    ) -> Tuple[str, str]:
        script = f"""
import pyautogui
pyautogui.FAILSAFE = False
ratio = {self.ratio}
x1 = {x1}
y1 = {y1}
x2 = {x2}
y2 = {y2}
x1, y1 = x1//ratio, y1//ratio
x2, y2 = x2//ratio, y2//ratio
pyautogui.moveTo(x1,y1)
pyautogui.mouseDown()
pyautogui.moveTo(x2,y2,duration=0.5)
pyautogui.mouseUp()
print('Action: drag from (%d, %d) to (%d, %d)' % (x1, y1, x2, y2))
"""

        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def replace(self, x: int, y: int, text: str) -> Tuple[str, str]:
        script = f"""
import pyautogui
import pyperclip
from pynput.mouse import Button, Controller
import re
pyautogui.FAILSAFE = False
ratio = {self.ratio}
ctrl_key = '{self.ctrl_key}'
x = {x}
y = {y}
text = '{text}'
x, y = x//ratio, y//ratio
print('Action: replace the content at (%d, %d) '
      'with %s and press Enter' % (x, y, text))
mouse = Controller()
pyautogui.moveTo(x,y)
mouse.click(Button.left, count=2)
shortcut('command', 'a')
pyperclip.copy(text)
pyautogui.keyDown(ctrl_key)
pyautogui.keyDown('v')
pyautogui.keyUp('v')
pyautogui.keyUp(ctrl_key)
time.sleep(0.5)
pyautogui.press('enter')
"""

        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def append(self, x: int, y: int, text: str) -> Tuple[str, str]:
        script = f"""
import pyautogui
import pyperclip
import re
from pynput.mouse import Button, Controller
pyautogui.FAILSAFE = False
def contains_chinese(text):
    return bool(re.search(r'[\u4e00-\u9fff]', text))
def shortcut(key1, key2):
    # if key1 == 'command' and args.pc_type != "mac":
    # key1 = 'ctrl'
    if key1 == 'command' or key1 == 'ctrl':
        key1 = ctrl_key
    print('Action: shortcut %s + %s' % (key1, key2))
    pyautogui.keyDown(key1)
    pyautogui.keyDown(key2)
    pyautogui.keyUp(key2)
    pyautogui.keyUp(key1)
    return
x = {x}
y = {y}
text = '{text}'
ctrl_key = '{self.ctrl_key}'
x, y = x//ratio, y//ratio
print('Action: append the content at (%d, %d) '
      'with %s and press Enter' % (x, y, text))
mouse = Controller()
pyautogui.moveTo(x,y)
mouse.click(Button.left, count=1)
shortcut('command', 'a')
pyautogui.press('down')
if contains_chinese(text):
    pyperclip.copy(text)
    pyautogui.keyDown(ctrl_key)
    pyautogui.keyDown('v')
    pyautogui.keyUp('v')
    pyautogui.keyUp(ctrl_key)
else:
    pyautogui.typewrite(text)
time.sleep(1)
pyautogui.press('enter')
"""

        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def mouse_move(self, x: int, y: int) -> Tuple[str, str]:
        script = f"""
import pyautogui
pyautogui.FAILSAFE = False
ratio = {self.ratio}
x = {x}
y = {y}
x, y = x//ratio, y//ratio
pyautogui.moveTo(x,y)
"""
        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def middle_click(self, x: int, y: int) -> Tuple[str, str]:
        script = f"""
import pyautogui
pyautogui.FAILSAFE = False
ratio = {self.ratio}
x = {x}
y = {y}
pyautogui.middleClick(x, y)
"""
        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def type_with_clear_enter(
        self,
        text: str,
        clear: int,
        enter: int,
    ) -> Tuple[str, str]:
        script = f"""
import pyautogui
import pyperclip
import time
ratio = {self.ratio}
ctrl_key = '{self.ctrl_key}'
text = '{text}'
clear = {clear}
enter = {enter}
if clear == 1:
    pyautogui.keyDown(ctrl_key)
    pyautogui.keyDown('a')
    pyautogui.keyUp('a')
    pyautogui.keyUp(ctrl_key)
    pyautogui.press('backspace')
    time.sleep(0.5)
pyperclip.copy(text)
pyautogui.keyDown(ctrl_key)
pyautogui.keyDown('v')
pyautogui.keyUp('v')
pyautogui.keyUp(ctrl_key)
time.sleep(0.5)
if enter == 1:
    pyautogui.press('enter')
"""
        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def type_with_clear_enter_pos(
        self,
        text: str,
        x: int,
        y: int,
        clear: int,
        enter: int,
    ) -> Tuple[str, str]:
        script = f"""
import pyautogui
import pyperclip
import time
ratio = {self.ratio}
ctrl_key = '{self.ctrl_key}'
text = '{text}'
x = {x}
y = {y}
clear = {clear}
enter = {enter}
x, y = x/ratio, y/ratio
pyautogui.click(x=x, y=y)
time.sleep(0.5)
if clear == 1:
    pyautogui.keyDown(ctrl_key)
    pyautogui.keyDown('a')
    pyautogui.keyUp('a')
    pyautogui.keyUp(ctrl_key)
    pyautogui.press('backspace')
    time.sleep(0.5)

pyperclip.copy(text)
pyautogui.keyDown(ctrl_key)
pyautogui.keyDown('v')
pyautogui.keyUp('v')
pyautogui.keyUp(ctrl_key)
time.sleep(0.5)
if enter == 1:
    pyautogui.press('enter')
"""
        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def scroll_pos(self, x: int, y: int, pixels: int) -> Tuple[str, str]:
        script = f"""
import pyautogui
import time
ratio = {self.ratio}
x = {x}
y = {y}
pixels = {pixels}*150
x, y = x//ratio, y//ratio
pyautogui.moveTo(x, y)
time.sleep(0.5)
pyautogui.scroll(pixels)
print('scroll_pos')
"""
        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)

    async def scroll(self, pixels: int) -> Tuple[str, str]:
        script = f"""
import pyautogui
pixels = {pixels}*150
pyautogui.scroll(pixels)
print('scroll')
"""
        full_python_command = f'\npython -c @"{script}"@'

        # 构造 PowerShell 命令
        command = (
            r'$env:Path += ";C:\Program Files\Python310"'
            f"{full_python_command}"
        )

        return await self.run_command_power_shell(command)


async def download_oss_image_and_save(
    oss_url: str,
    local_save_path: str,
) -> str:
    """
    根据OSS预签名URL下载图片，保存到本地，并返回Base64编码
    :param oss_url: str, OSS图片的预签名URL
    :param local_save_path: str, 本地保存路径（含文件名）
    :return: str, Base64编码的图片数据
    """
    try:
        # 下载图片
        async with aiohttp.ClientSession() as session:
            async with session.get(oss_url) as response:
                if response.status != 200:
                    raise Exception(
                        f"Download failed with status code {response.status}",
                    )
                content = await response.read()

        # 确保目录存在
        os.makedirs(os.path.dirname(local_save_path), exist_ok=True)

        # 保存到本地
        with open(local_save_path, "wb") as f:
            f.write(content)
        logger.info(f"Image saved to {local_save_path}")

        # 转换为Base64
        with open(local_save_path, "rb") as image_file:
            encoded_str = base64.b64encode(
                image_file.read(),
            ).decode("utf-8")

        return f"data:image/png;base64,{encoded_str}"

    except Exception as e:
        logger.error(f"Error downloading or saving image: {e}")
        return ""


class AppStreamClient:

    def __init__(self) -> None:
        config = open_api_models.Config(
            access_key_id=os.environ.get("ECD_ALIBABA_CLOUD_ACCESS_KEY_ID"),
            # 您的AccessKey Secret,
            access_key_secret=os.environ.get(
                "ECD_ALIBABA_CLOUD_ACCESS_KEY_SECRET",
            ),
        )
        # Endpoint 请参考 https://api.aliyun.com/product/eds-aic
        config.endpoint = (
            f"appstream-center."
            f'{os.environ.get("ECD_APP_STREAM_REGION_ID")}.aliyuncs.com'
        )
        self.__client__ = appstream_center20210218Client(config)

    async def search_auth_code(self) -> str:
        """获取新的auth_code，每次调用都会生成新的认证码"""
        get_auth_code_request = (
            appstream_center_20210218_models.GetAuthCodeRequest(
                end_user_id=os.environ.get("ECD_USERNAME"),
            )
        )
        runtime = util_models.RuntimeOptions()
        try:
            # 复制代码运行请自行打印 API 的返回值
            rep = await self.__client__.get_auth_code_with_options_async(
                get_auth_code_request,
                runtime,
            )
            auth_code = rep.body.auth_model.auth_code
            logger.info(f"成功获取新的auth_code: {auth_code[:20]}...")
            return auth_code
        except Exception as error:
            logger.error(f"search authcode failed:{error}")
            return ""
