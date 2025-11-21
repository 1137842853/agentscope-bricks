# -*- coding: utf-8 -*-
import os
import datetime
import requests
import json
import re
from PIL import Image
from typing import Optional, Any, AsyncGenerator, Union

from agentscope_bricks.utils.grounding_utils import draw_point, encode_image
from pathlib import Path
import time  # 添加 time 模块导入
from agentscope_runtime.sandbox.box.cloud_api.utils.utils import (
    get_image_size_from_url,
)
from agentscope_runtime.sandbox.box.cloud_api.cloud_phone_sandbox import (
    CloudPhoneSandbox,
)
from agentscope_runtime.sandbox.box.cloud_api.cloud_computer_sandbox import (
    CloudComputerSandbox,
)
import asyncio

from uuid import uuid4

# AgentScope imports
from agentscope_runtime.engine.agents.base_agent import Agent
from agentscope_runtime.engine.schemas.agent_schemas import (
    Content,
    Message,
)
from agents.agent import DataContent
from agentscope_runtime.engine.schemas.context import Context
from agents.gui_agent_app_v2 import GuiAgent
from agentscope_bricks.utils.logger_util import logger

TYPING_DELAY_MS = 12
TYPING_GROUP_SIZE = 50
HUMAN_HELP_ACTION = "human_help"
gui_agent = GuiAgent()


class GreenNetInterceptionError(RuntimeError):
    """绿网拦截错误，需要立即终止任务"""

    def __init__(self, message, request_id=None, error_code=None):
        super().__init__(message)
        self.request_id = request_id
        self.error_code = error_code
        self.is_green_net_error = True


# 资源池配置 - 从环境变量或默认值获取
PHONE_INSTANCE_IDS = (
    os.getenv("PHONE_INSTANCE_IDS", "").split(",")
    if os.getenv("PHONE_INSTANCE_IDS")
    else []
)
DESKTOP_IDS = (
    os.getenv("DESKTOP_IDS", "").split(",") if os.getenv("DESKTOP_IDS") else []
)


class ComputerUseAgent(Agent):
    def __init__(
        self,
        name: str = "ComputerUseAgent",
        agent_config: Optional[dict] = None,
    ):
        super().__init__(name=name, agent_config=agent_config)

        # Extract parameters from agent_config
        config = agent_config or {}
        equipment = config.get("equipment")
        # output_dir = config.get("output_dir", ".")
        mode = config.get("mode", "pc_use")
        sandbox_type = config.get("sandbox_type", "pc_wuyin")
        status_callback = config.get("status_callback")
        pc_use_add_info = config.get("pc_use_add_info", "")
        max_steps = config.get("max_steps", 20)
        chat_id = config.get("chat_id", "")
        user_id = config.get("user_id", "")
        e2e_info = config.get("e2e_info", [])
        extra_params = config.get("extra_params", "")
        state_manager = config.get("state_manager")  # 新增：获取状态管理器

        # Save initialization parameters for copy method
        self._attr = {
            "name": name,
            "agent_config": self.agent_config,
        }

        # Initialize computer use specific attributes
        self.chat_instruction = None
        self.latest_screenshot = None  # Most recent PNG of the screen
        self.image_counter = 0  # Current screenshot number
        # Store state manager for dynamic equipment access
        self.state_manager = state_manager

        # Equipment can be None initially if using dynamic allocation
        self.equipment = equipment

        # Setup output directory based on chat_id and timestamp
        time_now = datetime.datetime.now()
        # 在Docker容器中使用相对路径，确保目录在容器内正确创建
        self.tmp_dir = os.path.join(
            "output",
            user_id,
            chat_id,
            time_now.strftime("%Y%m%d_%H%M%S"),
        )

        # 确保基础output目录存在
        try:
            base_output_dir = "output"
            if not os.path.exists(base_output_dir):
                os.makedirs(base_output_dir, exist_ok=True)
                print(f"Created base output directory: {base_output_dir}")
        except Exception as e:
            print(f"Warning: Failed to create base output directory: {e}")

        # Configuration
        self.mode = mode
        self.sandbox_type = sandbox_type
        self.status_callback = status_callback
        self.max_steps = max_steps
        self.user_id = user_id
        self.chat_id = chat_id
        self.e2e_info = e2e_info
        self.extra_params = extra_params

        # Setup sandbox reference
        if hasattr(self.equipment, "device") and self.equipment.device:
            self.sandbox = self.equipment.device

        print(f"e2e_info: {e2e_info}")

        # Mode-specific setup
        if mode == "pc_use":
            self.session_id = ""
            self.add_info = pc_use_add_info
        elif mode == "phone_use":
            if self.sandbox_type == "phone_wuyin":
                self.session_id = ""
                self.add_info = pc_use_add_info
        else:
            logger.error("Invalid mode")
            raise ValueError(
                f"Invalid mode: {mode}, must be one of: [pc_use, phone_use]",
            )

        # Control flags
        self._is_cancelled = False
        self._interrupted = False
        # Background wait task management
        self._wait_task = None

    async def _ensure_equipment(self):
        """确保设备可用，如果没有则从状态管理器获取"""
        if self.equipment is None and self.state_manager is not None:
            try:
                logger.info(
                    f"尝试从状态管理器获取设备信息，对话ID: {self.chat_id}",
                )

                equipment_info = await self.state_manager.get_equipment_info(
                    self.user_id,
                    self.chat_id,
                )

                if equipment_info:
                    self.equipment = await self._initialize_device_from_info(
                        equipment_info,
                    )
                    self._setup_sandbox_reference()
                    logger.info(
                        f"✅ 成功重建设备对象: {equipment_info['equipment_type']}",
                    )
                    return True
                else:
                    await self._handle_missing_equipment()

            except Exception as e:
                logger.error(f"获取设备失败: {str(e)}")
                raise Exception(f"无法获取设备: {str(e)}")

        return self.equipment is not None

    async def _initialize_device_from_info(self, equipment_info):
        """根据设备信息初始化设备对象"""
        equipment_type = equipment_info["equipment_type"]
        instance_info = equipment_info["instance_manager_info"]

        if equipment_type == "pc_wuyin":
            return await self._create_device(
                CloudComputerSandbox,
                instance_info["desktop_id"],
            )
        elif equipment_type == "phone_wuyin":
            return await self._create_device(
                CloudPhoneSandbox,
                instance_info["instance_id"],
            )
        else:
            raise Exception(f"不支持的设备类型: {equipment_type}")

    async def _create_device(self, device_class, device_id):
        """创建设备实例，自动处理事件循环问题"""
        try:
            if device_class == CloudComputerSandbox:
                device = CloudComputerSandbox(desktop_id=device_id)
            else:  # CloudPhoneSandbox
                device = CloudPhoneSandbox(instance_id=device_id)
            return device
        except RuntimeError as e:
            if "There is no current event loop" in str(
                e,
            ) or "got Future" in str(e):
                logger.warning(f"检测到事件循环问题，使用线程池初始化: {e}")
                return await asyncio.to_thread(
                    lambda: self._sync_init_device(device_class, device_id),
                )
            raise
        except Exception as e:
            logger.error(f"{device_class.__name__}设备初始化失败: {str(e)}")
            raise Exception(f"{device_class.__name__}设备初始化失败: {str(e)}")

    def _sync_init_device(self, device_class, device_id):
        """在新事件循环中同步初始化设备"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            if device_class == CloudComputerSandbox:
                device = CloudComputerSandbox(desktop_id=device_id)
            else:
                device = CloudPhoneSandbox(instance_id=device_id)
            return device
        finally:
            loop.close()

    def _setup_sandbox_reference(self):
        """设置sandbox引用"""
        if hasattr(self.equipment, "device") and self.equipment.device:
            self.sandbox = self.equipment.device

    async def _handle_missing_equipment(self):
        """处理设备信息缺失的情况"""
        chat_state = await self.state_manager.get_chat_state(
            self.user_id,
            self.chat_id,
        )
        storage_status = chat_state.get("equipment_storage_status")

        if storage_status == "e2b_desktop":
            raise Exception("E2B设备无法从Redis恢复，请重新初始化设备")
        elif storage_status == "stored_in_redis":
            raise Exception("设备信息已过期，请重新初始化设备")
        else:
            raise Exception("设备未初始化，请先通过 /cua/init 接口初始化设备")

    async def _check_stop_signal(self):
        """检查Redis中的停止信号 - 简化版本，避免事件循环冲突"""
        try:
            # 如果没有状态管理器，直接返回False
            if not self.state_manager:
                return False

            # 直接使用现有的状态管理器检查停止信号，而不是创建新的
            if hasattr(self.state_manager, "check_stop_signal"):
                stop_requested = await self.state_manager.check_stop_signal(
                    self.user_id,
                    self.chat_id,
                )

                if stop_requested:
                    # 清除停止信号
                    await self.state_manager.clear_stop_signal(
                        self.user_id,
                        self.chat_id,
                    )
                    # 发送停止确认状态
                    await self.state_manager.update_status(
                        self.user_id,
                        self.chat_id,
                        {
                            "status": "stopped",
                            "message": "Agent has received stop signal"
                            " and is terminating",
                            "type": "SYSTEM",
                        },
                    )
                    logger.info("检测到停止信号，设置取消标志")

                return stop_requested
            else:
                # 如果状态管理器不支持停止信号检查，返回False
                return False

        except Exception as e:
            logger.error(f"检查停止信号时出错: {e}")
            return False

    def copy(self) -> "ComputerUseAgent":
        """Create a copy of the agent"""
        return ComputerUseAgent(**self._attr)

    async def run_async(
        self,
        context: Context,
        **kwargs: Any,
    ) -> AsyncGenerator[Union[Message, Content], None]:
        """
        AgentScope async run method that yields streaming responses
        """
        # 确保设备可用
        try:
            await self._ensure_equipment()
        except Exception as e:
            error_msg = f"设备不可用: {str(e)}"
            # logger.error(error_msg)
            logger.error(error_msg)
            yield DataContent(
                data={
                    "step": "",
                    "stage": "error",
                    "type": "text",
                    "text": error_msg,
                },
            )
            return

        request = context.request
        if not request or not request.input:
            error_msg = "No input found in request."
            # logger.error(error_msg)
            print(error_msg)
            yield DataContent(
                data={
                    "step": "",
                    "stage": "error",
                    "type": "text",
                    "text": f"Error: {error_msg}",
                },
            )
            return

        # Extract instruction from the first input message
        first_message = request.input[0]
        if hasattr(first_message, "content") and isinstance(
            first_message.content,
            list,
        ):
            # Find text content in the message
            instruction = None
            for content_item in first_message.content:
                if (
                    hasattr(content_item, "type")
                    and content_item.type == "text"
                ):
                    instruction = content_item.text
                    break
        else:
            instruction = (
                str(first_message.content)
                if hasattr(first_message, "content")
                else str(first_message)
            )

        if not instruction:
            error_msg = "No instruction text found in input message."
            # logger.error(error_msg)
            logger.error(error_msg)
            yield DataContent(
                data={
                    "step": "",
                    "stage": "error",
                    "type": "text",
                    "text": f"Error: {error_msg}",
                },
            )
            return

        # Update chat_id from session if available
        session_id = (
            request.session_id if hasattr(request, "session_id") else None
        )
        if session_id:
            self.chat_id = session_id

        # Yield initial task start message
        yield DataContent(
            data={
                "step": "",
                "stage": "start",
                "type": "text",
                "text": f"🤖 开始执行任务: {instruction}",
            },
        )
        # 清楚上一次的停止信号
        await self.state_manager.clear_stop_signal(self.user_id, self.chat_id)
        # Run the main computer use task loop
        async for result in self._execute_computer_use_task(instruction):
            yield result

    async def _execute_computer_use_task(
        self,
        instruction: str,
    ) -> AsyncGenerator[Union[Message, Content], None]:
        """
        Execute computer use task with streaming responses
        """
        logger.info("Running task...")
        try:
            # cjj疑似改
            while not self._is_cancelled:
                self.chat_instruction = instruction
                logger.info(f"USER: {instruction}")
                if self.mode in ["pc_use", "phone_use"]:
                    self.session_id = ""

                should_continue = True
                step_count = 0
                while should_continue and step_count < self.max_steps:
                    # 检查取消标志
                    if self._is_cancelled:
                        break

                    # 检查Redis中的停止信号
                    if self.state_manager and await self._check_stop_signal():
                        logger.info("收到Redis停止信号，终止任务")
                        self._is_cancelled = True
                        break

                    step_count += 1

                    # Yield step start message
                    yield DataContent(
                        data={
                            "step": f"{step_count}",
                            "stage": "output",
                            "type": "text",
                            "text": f"🔄 第 {step_count} 步",
                        },
                    )
                    step_info = {
                        "step": step_count,
                        "auxiliary_info": {},
                        "observation": "",
                        "action_parsed": "",
                        "action_executed": "",
                        "timestamp": time.time(),
                        "uuid": str(uuid4()),
                    }
                    # 添加设备ID信息
                    equipment_id = "Unknown"
                    if hasattr(self.equipment, "instance_manager"):
                        if hasattr(
                            self.equipment.instance_manager,
                            "desktop_id",
                        ):
                            equipment_id = (
                                self.equipment.instance_manager.desktop_id
                            )
                        elif hasattr(
                            self.equipment.instance_manager,
                            "instance_id",
                        ):
                            equipment_id = (
                                self.equipment.instance_manager.instance_id
                            )
                    equipment = self.equipment.instance_manager
                    step_info["equipment_id"] = equipment_id

                    # 发送推理开始状态
                    step_info["analyzing"] = True
                    logger.info("开始推理")
                    time_s_agent = time.time()

                    try:
                        # Yield analysis start message
                        yield DataContent(
                            data={
                                "step": f"{step_count}",
                                "stage": "output",
                                "type": "text",
                                "text": "🔍 分析屏幕截图",
                            },
                        )

                        # Process analyse_screenshot as async generator
                        screenshot_analysis = None
                        auxiliary_info = None
                        mode_response = None

                        async for data_content in self.analyse_screenshot(
                            step_count,
                        ):
                            # Yield status updates
                            if (
                                data_content.data.get("type")
                                == "analysis_result"
                            ):
                                # Extract final results
                                screenshot_analysis = data_content.data.get(
                                    "text",
                                )
                                auxiliary_info = data_content.data.get(
                                    "auxiliary_info",
                                )
                                mode_response = data_content.data.get(
                                    "mode_response",
                                )
                                yield DataContent(
                                    data={
                                        "step": f"{step_count}",
                                        "stage": "draw",
                                        "type": "analysis_result",
                                        "text": f"{screenshot_analysis}",
                                        "auxiliary_info": auxiliary_info,
                                    },
                                )
                            else:
                                yield data_content

                    except GreenNetInterceptionError as green_net_error:
                        # 绿网拦截错误：立即终止任务
                        error_msg = str(green_net_error)
                        logger.error(
                            f"绿网拦截错误，立即终止任务: {error_msg}",
                        )
                        self._is_cancelled = True  # 立即设置取消标志
                        should_continue = False  # 终止内层循环
                        yield DataContent(
                            data={
                                "step": f"{step_count}",
                                "stage": "error",
                                "type": "text",
                                "text": error_msg,
                            },
                        )
                        # 发送任务终止消息
                        yield DataContent(
                            data={
                                "step": "",
                                "stage": "canceled",
                                "type": "text",
                                "text": "⏹️ 任务因内容安全检测已终止",
                            },
                        )
                        break  # 立即退出内层循环
                    except Exception as analyse_error:
                        error_msg = f"Analysis failed: {str(analyse_error)}"
                        logger.error(error_msg)
                        yield DataContent(
                            data={
                                "step": f"{step_count}",
                                "stage": "error",
                                "type": "text",
                                "text": f"错误: {error_msg}",
                            },
                        )
                        raise analyse_error

                    logger.info(
                        "screenshot analysis "
                        f"cost time{time.time() - time_s_agent}",
                    )

                    # 推理完成，更新完整信息
                    step_info["observation"] = screenshot_analysis
                    step_info["analyzing"] = False
                    if auxiliary_info:
                        step_info["auxiliary_info"].update(auxiliary_info)

                    # Yield action execution message
                    yield DataContent(
                        data={
                            "step": f"{step_count}",
                            "stage": "output",
                            "type": "text",
                            "text": "⚡ 执行操作",
                        },
                    )

                    if self.status_callback:
                        self.emit_status("STEP", step_info)

                    # 使用try-catch包围设备操作，防止异步问题
                    try:
                        if self.mode == "pc_use":
                            action_result = await self._execute_pc_action(
                                mode_response,
                                equipment,
                                step_count,
                            )
                            # 处理人工干预
                            if action_result.get("human_intervention"):
                                yield DataContent(
                                    data=action_result["human_intervention"],
                                )

                                # 如果需要等待人工干预，开始等待
                                if action_result["result"] == "wait_for_human":
                                    wait_time = action_result.get(
                                        "wait_time",
                                        60,
                                    )
                                    self._wait_task = asyncio.create_task(
                                        self._do_wait_for_human_help(
                                            wait_time,
                                        ),
                                    )

                                    try:
                                        await self._wait_task
                                    except asyncio.CancelledError:
                                        logger.info(
                                            "PC wait task was cancelled",
                                        )

                            if action_result["result"] == "stop":
                                should_continue = False
                                yield DataContent(
                                    data={
                                        "step": f"{step_count}",
                                        "stage": "completed",
                                        "type": "text",
                                        "text": "步骤完成!",
                                    },
                                )
                                self._is_cancelled = True
                            if "Answer" in action_result["result"]:
                                should_continue = False
                                yield DataContent(
                                    data={
                                        "step": f"{step_count}",
                                        "stage": "completed",
                                        "type": "text",
                                        "text": action_result["result"],
                                    },
                                )
                                self._is_cancelled = True
                        elif self.mode == "phone_use":
                            action_result = await self._execute_phone_action(
                                mode_response,
                                equipment,
                                auxiliary_info,
                                step_count,
                            )
                            # 处理人工干预
                            if action_result.get("human_intervention"):
                                yield DataContent(
                                    data=action_result["human_intervention"],
                                )

                                # 如果需要等待人工干预，开始等待
                                if action_result["result"] == "wait_for_human":
                                    wait_time = action_result.get(
                                        "wait_time",
                                        60,
                                    )
                                    self._wait_task = asyncio.create_task(
                                        self._do_wait_for_human_help(
                                            wait_time,
                                        ),
                                    )

                                    try:
                                        await self._wait_task
                                    except asyncio.CancelledError:
                                        print(
                                            "Phone wait task was cancelled by "
                                            "user intervention",
                                        )

                            if action_result["result"] == "stop":
                                should_continue = False
                                yield DataContent(
                                    data={
                                        "step": f"{step_count}",
                                        "stage": "completed",
                                        "type": "text",
                                        "text": "步骤完成!",
                                    },
                                )
                                self._is_cancelled = True

                    except GreenNetInterceptionError as green_net_error:
                        # 绿网拦截错误：立即终止任务
                        error_msg = str(green_net_error)
                        logger.error(
                            f"执行操作时遇到绿网拦截错误，立即终止任务: {error_msg}",
                        )
                        self._is_cancelled = True  # 立即设置取消标志
                        should_continue = False  # 终止内层循环
                        yield DataContent(
                            data={
                                "step": f"{step_count}",
                                "stage": "error",
                                "type": "text",
                                "text": error_msg,
                            },
                        )
                        yield DataContent(
                            data={
                                "step": "",
                                "stage": "canceled",
                                "type": "text",
                                "text": "⏹️ 任务因内容安全检测已终止",
                            },
                        )
                        break  # 立即退出内层循环
                    except Exception as action_error:
                        error_msg = f"执行操作时出错: {str(action_error)}"
                        logger.error(error_msg)
                        yield DataContent(
                            data={
                                "step": f"{step_count}",
                                "stage": "error",
                                "type": "text",
                                "text": f"{error_msg}",
                            },
                        )
                        continue

                if not should_continue:
                    yield DataContent(
                        data={
                            "step": "",
                            "stage": "all_completed",
                            "type": "text",
                            "text": f"任务完成! 总共执行了 {step_count} 步",
                        },
                    )
                    break
                elif step_count >= self.max_steps:
                    yield DataContent(
                        data={
                            "step": "",
                            "stage": "limit_completed",
                            "type": "text",
                            "text": f"达到最大步数限制 ({self.max_steps})，任务停止",
                        },
                    )
                    break
                elif self._is_cancelled:
                    logger.info("✅ Task canceled")
                    yield DataContent(
                        data={
                            "step": "",
                            "stage": "canceled",
                            "type": "text",
                            "text": "⏹️ 任务已取消",
                        },
                    )
                    break

        except GreenNetInterceptionError as green_net_error:
            # 绿网拦截错误：任务已终止
            # 注意：错误消息可能已经在内层循环中发送过了，这里只做最终确认
            error_msg = str(green_net_error)
            logger.error(f"任务因绿网拦截终止: {error_msg}")
            self._is_cancelled = True  # 确保取消标志已设置

            # 如果内层循环没有发送消息（理论上不应该发生），这里作为兜底
            # 但通常内层循环已经发送过了，所以这里只发送一个确认消息
            yield DataContent(
                data={
                    "step": "",
                    "stage": "canceled",
                    "type": "text",
                    "text": "⏹️ 任务因内容安全检测已终止",
                },
            )
            # 确保调用stop方法清理资源
            self.stop()
        except Exception as e:
            error_msg = str(e)
            # 解析错误信息，特别处理绿网拦截错误
            error_info = self._parse_gui_service_error(e)

            # 检查是否为GUI服务请求失败的错误（包括绿网拦截）
            if (
                "GUI服务请求失败" in error_msg
                or error_info["is_green_net_error"]
            ):
                # 使用解析后的友好错误信息
                formatted_error = error_info["formatted_message"]
                # 如果是绿网拦截错误，设置取消标志
                if error_info["is_green_net_error"]:
                    self._is_cancelled = True
            else:
                formatted_error = f"执行任务时出错: {error_msg}"

            logger.error(f"执行任务时出错: {error_msg}")
            yield DataContent(
                data={
                    "step": "",
                    "stage": "error",
                    "type": "text",
                    "text": formatted_error,
                },
            )
        finally:
            self.stop()
            # 异步删除临时文件夹
            if os.path.exists(self.tmp_dir):
                import shutil

                async def cleanup_temp_dir():
                    try:
                        await asyncio.to_thread(shutil.rmtree, self.tmp_dir)
                    except Exception as e:
                        logger.info(
                            f"Failed to delete {self.tmp_dir}. Reason: {e}",
                        )

                # 在后台执行清理，不阻塞任务结束
                asyncio.create_task(cleanup_temp_dir())

            logger.info("Agent run loop exited.")

    def stop(self):
        print("Agent stopped by user request.")
        self._is_cancelled = True
        # 取消等待任务（如果存在）
        if self._wait_task and not self._wait_task.done():
            self._wait_task.cancel()
            logger.info("等待任务已取消")
        # 发送状态更新到前端
        self.emit_status(
            "SYSTEM",
            {
                "message": "Stop request received, waiting "
                "for current step to complete...",
                "status": "running",
            },
        )

    def interrupt_wait(self):
        """
        由前端调用，用于中断当前的等待状态
        """
        self._interrupted = True
        # 取消后台等待任务
        if self._wait_task and not self._wait_task.done():
            self._wait_task.cancel()
            logger.info("Background wait task cancelled")
        logger.info("Agent wait stopped by user request.")
        # 发送状态更新到前端
        self.emit_status(
            "SYSTEM",
            {
                "message": "Stop wait request received,"
                " waiting for current step to complete...",
                "status": "running",
            },
        )

    def emit_status(self, status_type: str, data: dict):
        """发射状态更新"""
        status_data = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type": status_type,
            "status": data.get("status", "running"),
            "data": data,
        }

        logger.info(
            f"emit_status - chat_id: {self.chat_id}, type: {status_type}",
        )

        if self.status_callback:
            try:
                if asyncio.iscoroutinefunction(self.status_callback):
                    self._run_async_callback(self.chat_id, status_data)
                else:
                    result = self.status_callback(self.chat_id, status_data)
                    logger.info(f"同步回调函数执行结果: {result}")
            except Exception as e:
                logger.error(f"Error in status callback: {e}")
        else:
            logger.warning("No status callback available")

    async def annotate_image(
        self,
        point: list,
        anno_type: str,
        is_save: bool = False,
    ):
        """异步图片标注"""

        def _create_annotation():
            from PIL import Image, ImageDraw

            if anno_type == "point":
                annotated_img = draw_point(
                    Image.open(self.latest_screenshot),
                    point,
                )
                return annotated_img
            elif anno_type == "box":
                coor = point
                # 四个点的坐标
                points = [
                    (coor[0], coor[1]),
                    (coor[0], coor[3]),
                    (coor[2], coor[3]),
                    (coor[2], coor[1]),
                ]
                image = Image.open(self.latest_screenshot)
                draw = ImageDraw.Draw(image)
                # 绘制四边形
                draw.polygon(
                    points,
                    outline="red",
                    fill=None,
                    width=3,
                )
                return image  # 返回图像对象，不是draw对象

            elif anno_type == "arrow":
                import math

                image = Image.open(self.latest_screenshot)
                draw = ImageDraw.Draw(image)
                [x1, y1, x2, y2] = point
                arrow_size = 10
                color = "red"
                draw.line((x1, y1, x2, y2), fill=color, width=2)

                # 计算箭头的方向向量
                angle = math.atan2(y2 - y1, x2 - x1)

                # 计算箭头的两个顶点
                arrow_x1 = x2 - arrow_size * math.cos(angle - math.pi / 6)
                arrow_y1 = y2 - arrow_size * math.sin(angle - math.pi / 6)
                arrow_x2 = x2 - arrow_size * math.cos(angle + math.pi / 6)
                arrow_y2 = y2 - arrow_size * math.sin(angle + math.pi / 6)

                # 绘制箭头（三角形）
                draw.polygon(
                    [(x2, y2), (arrow_x1, arrow_y1), (arrow_x2, arrow_y2)],
                    fill=color,
                )
                return image  # 返回图像对象，不是draw对象

            # 如果anno_type不匹配，返回原始图像
            return Image.open(self.latest_screenshot)

        annotated_img = await asyncio.to_thread(_create_annotation)

        screenshot_filename = os.path.basename(self.latest_screenshot)
        p = Path(screenshot_filename)  # 保留前面的目录
        oss_screenshot_filename = f"{p.stem}_{uuid4().hex}{p.suffix}"

        img_path = None
        oss_url = None

        if is_save:
            try:
                img_path = await self.save_image(
                    annotated_img,
                    f"{screenshot_filename[:-4]}_annotated",
                )
                logger.info(f"[DEBUG] Annotated image saved to: {img_path}")
            except Exception as e:
                logger.error(f"Failed to save annotated image: {e}")

        # 只有在成功保存图片时才上传
        if img_path:
            try:
                # 异步上传到oss
                async def _upload_to_oss():
                    ei_c = self.equipment.instance_manager.oss_client
                    method = ei_c.oss_upload_file_and_sign_async
                    return await method(
                        img_path,
                        oss_screenshot_filename,
                    )

                oss_url = await _upload_to_oss()
                logger.info(
                    f"[DEBUG] Annotated image uploaded to OSS: {oss_url}",
                )
            except Exception as e:
                logger.info(
                    f"Failed to upload annotated image to OSS: {e}",
                )
                oss_url = None
        else:
            logger.info("[DEBUG] No image path, skipping OSS upload")

        return encode_image(annotated_img), oss_url

    def _run_async_callback(self, chat_id, status_data):
        """简化的异步回调处理"""
        if not self.status_callback:
            return

        try:
            import asyncio

            try:
                loop = asyncio.get_running_loop()
                task = loop.create_task(
                    self.status_callback(chat_id, status_data),
                )
                logger.info(f"Created async task for status callback: {task}")
            except RuntimeError:
                logger.warning(
                    "No running event loop, calling callback synchronously",
                )
                try:
                    asyncio.run(self.status_callback(chat_id, status_data))
                except Exception as sync_error:
                    logger.error(
                        f"Sync callback execution failed: {sync_error}",
                    )
                    try:
                        self.status_callback(chat_id, status_data)
                    except Exception as fallback_error:
                        logger.error(
                            f"Fallback callback also failed: {fallback_error}",
                        )
        except Exception as e:
            logger.error(f"Error running async callback: {e}")

    async def save_image(self, image, prefix="image"):
        """异步保存图片"""

        def _save_sync():
            if not os.path.exists(self.tmp_dir):
                os.makedirs(self.tmp_dir)

            # 使用随机数命名文件，避免重复
            random_suffix = uuid4().hex[:8]
            filename = f"{prefix}_{random_suffix}.png"
            filepath = os.path.join(self.tmp_dir, filename)

            if isinstance(image, Image.Image):
                image.save(filepath)
            else:
                with open(filepath, "wb") as f:
                    f.write(image)
            return filepath

        return await asyncio.to_thread(_save_sync)

    async def take_screenshot(self, prefix="screenshot"):
        """统一的截图方法，自动根据设备类型选择适当的截图方式"""
        if self.sandbox_type == "pc_wuyin":
            return await self._screenshot_pc(prefix)
        elif self.sandbox_type == "phone_wuyin":
            return await self._screenshot_phone(prefix)
        else:
            return await self._screenshot_default(prefix)

    async def _screenshot_pc(self, prefix):
        """电脑截图处理"""
        try:
            filepath, filename = self._prepare_screenshot_path(prefix)
            await self.equipment.get_screenshot_base64_save_local_async(
                filename.replace(".png", ""),
                filepath,
            )

            oss_url = await self._upload_to_oss(filepath)
            self.latest_screenshot = filepath

            image_data = await self._read_image_file(filepath)
            return image_data, oss_url, filename
        except Exception as e:
            raise Exception(f"电脑截图失败: {str(e)}")

    async def _screenshot_phone(self, prefix):
        """手机截图处理"""
        try:
            filepath, filename = self._prepare_screenshot_path(prefix)
            oss_url = await self.equipment.get_screenshot_oss_phone_async()

            # 下载图片到本地
            await self._download_image(oss_url, filepath)

            # 重新上传到OSS获取新URL
            new_oss_url = await self._upload_to_oss(filepath)
            self.latest_screenshot = filepath

            image_data = await self._read_image_file(filepath)
            return image_data, new_oss_url, filename
        except Exception as e:
            raise Exception(f"手机截图失败: {str(e)}")

    async def _screenshot_default(self, prefix):
        """默认截图处理（e2b等）"""
        file = await asyncio.to_thread(self.sandbox.screenshot)
        filename = await self.save_image(file, prefix)
        self.latest_screenshot = filename

        image_data = await self._read_image_file(filename)
        oss_url = encode_image(file)
        return image_data, oss_url, os.path.basename(filename)

    def _prepare_screenshot_path(self, prefix):
        """准备截图文件路径"""
        os.makedirs(self.tmp_dir, exist_ok=True)
        random_suffix = uuid4().hex[:8]
        filename = f"{prefix}_{random_suffix}.png"
        filepath = os.path.join(self.tmp_dir, filename)
        return filepath, filename

    async def _upload_to_oss(self, filepath):
        """上传文件到OSS"""
        p = Path(filepath)
        oss_filepath = f"{p.stem}_{uuid4().hex}{p.suffix}"
        return await self.equipment.oss_client.oss_upload_file_and_sign_async(
            filepath,
            oss_filepath,
        )

    async def _download_image(self, url, filepath):
        """下载图片到本地"""

        def download():
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(1024):
                        f.write(chunk)
            else:
                raise Exception(f"Failed to download image from {url}")

        await asyncio.to_thread(download)

    async def _read_image_file(self, filepath):
        """读取图片文件"""

        def read_file():
            with open(filepath, "rb") as f:
                return f.read()

        return await asyncio.to_thread(read_file)

    # def use_upload_local_file_oss(self, file_path, file_name):
    #     with open(file_path, "rb") as file:
    #         return self.equipment.upload_local_file_oss(file, file_name)

    def _parse_gui_service_error(self, error):
        """
        解析GUI服务错误，特别处理绿网拦截错误
        返回格式化的错误信息字典
        """
        error_str = str(error)
        error_dict = {
            "is_green_net_error": False,
            "formatted_message": error_str,
            "request_id": None,
            "error_code": None,
        }

        # 检查是否为绿网拦截错误
        if (
            "DataInspectionFailed" in error_str
            or "inappropriate content" in error_str.lower()
        ):
            error_dict["is_green_net_error"] = True
            error_dict["error_code"] = "DataInspectionFailed"

            # 尝试提取request_id
            request_id_match = re.search(r'"request_id":"([^"]+)"', error_str)
            if request_id_match:
                error_dict["request_id"] = request_id_match.group(1)

            # 生成友好的错误提示
            if error_dict["request_id"]:
                error_dict["formatted_message"] = (
                    f"⚠️ 内容安全检测：输入内容可能包含不当信息，已被系统拦截。"
                    f"请求ID: {error_dict['request_id']}。"
                    f"请检查输入内容或稍后重试。"
                )
            else:
                error_dict["formatted_message"] = (
                    "⚠️ 内容安全检测：输入内容可能包含不当信息，已被系统拦截。"
                    "请检查输入内容或稍后重试。"
                )
        # 检查是否为GUI服务请求失败
        elif "GUI服务请求失败" in error_str or "Error querying" in error_str:
            # 尝试提取request_id
            request_id_match = re.search(r'"request_id":"([^"]+)"', error_str)
            if request_id_match:
                error_dict["request_id"] = request_id_match.group(1)
                error_dict["formatted_message"] = (
                    f"内部agent调用异常，请求ID: {error_dict['request_id']}"
                )
            else:
                error_dict["formatted_message"] = "内部agent调用异常"

        return error_dict

    def _handle_action_error(self, error, action_type="action"):
        """处理动作执行错误的通用方法"""
        error_msg = f"Error in {action_type}: {str(error)}"
        logger.error(error_msg)
        import traceback

        traceback.print_exc()
        return {"result": "error", "error": str(error)}

    async def _handle_human_intervention(self, task, step_count):
        """处理人工干预的通用方法"""
        try:
            human_intervention_info = await self._wait_for_human_help(
                task,
                step_count,
            )
            return {
                "result": "wait_for_human",
                "human_intervention": human_intervention_info,
                "wait_time": int(os.getenv("HUMAN_WAIT_TIME", "60")),
            }
        except Exception as e:
            logger.error(f"人工干预处理出错: {str(e)}")
            error_intervention_info = {
                "step": f"{step_count}" if step_count else "",
                "stage": "human_help",
                "type": "human_intervention",
                "text": f"人工干预处理出错: {str(e)}",
                "task_description": task if "task" in locals() else "未知任务",
                "wait_time": 0,
                "timestamp": time.time(),
                "uuid": str(uuid4()),
            }
            return {
                "result": "continue",
                "human_intervention": error_intervention_info,
            }

    async def _wait_for_human_help(self, task, step_count=None):
        """
        异步等待人类帮助完成任务的通用方法
        立即返回人工干预信息，然后开始等待
        """
        time_to_sleep = int(os.getenv("HUMAN_WAIT_TIME", "60"))  # 转换为整数
        logger.info(
            "HUMAN_HELP: The system will wait "
            f"for {time_to_sleep} "
            f"seconds for human to do the task: {task}",
        )

        # 立即返回人工干预信息，让前端马上显示
        human_intervention_info = {
            "step": f"{step_count}" if step_count else "",
            "stage": "human_help",
            "type": "human_intervention",
            "text": f"需要人工干预: {task}",
            "task_description": task,
            "wait_time": time_to_sleep,
            "timestamp": time.time(),
            "uuid": str(uuid4()),
        }

        return human_intervention_info

    async def _do_wait_for_human_help(self, time_to_sleep):
        """
        实际执行等待逻辑的方法 - 简化版本，避免事件循环问题
        """
        # 重置中断标志
        self._interrupted = False
        start_time = time.time()
        sleep_interval = min(5, time_to_sleep)  # 每次最多等待5秒

        # 简化的等待循环，减少复杂性
        while (
            (time.time() - start_time) < time_to_sleep
            and not self._interrupted
            and not self._is_cancelled
        ):
            await asyncio.sleep(sleep_interval)

            # 简单检查停止信号，不创建新的连接
            try:
                if self.state_manager and hasattr(
                    self.state_manager,
                    "check_stop_signal",
                ):
                    if await self.state_manager.check_stop_signal(
                        self.user_id,
                        self.chat_id,
                    ):
                        logger.info("等待人工干预时收到停止信号")
                        self._is_cancelled = True
                        break
            except Exception as e:
                logger.error(f"检查停止信号时出错: {e}")
                # 继续等待，不因为这个错误而终止

        waited_time = time.time() - start_time

        if self._interrupted:
            logger.info("Human help wait was interrupted by user.")
            self._interrupted = False  # 重置标志
        elif self._is_cancelled:
            logger.info("Human help wait was cancelled.")
        else:
            logger.info(
                f"Human help wait completed after {waited_time:.1f}s",
            )

    async def analyse_screenshot(self, step_count: int = None):
        auxiliary_info = {}
        try:
            # 发送截图阶段状态
            yield DataContent(
                data={
                    "step": f"{step_count}",
                    "stage": "screenshot",
                    "type": "analysis_stage",
                    "text": "capturing",
                    "timestamp": time.time(),
                    "uuid": str(uuid4()),
                },
            )

            if self.sandbox_type == "pc_wuyin":
                time_s = time.time()
                logger.info(
                    f"执行中analyse_screenshot_instance_id:"
                    f" {self.equipment.instance_manager.desktop_id}",
                )
                screenshot_img, screenshot_oss, screenshot_filename = (
                    await self.take_screenshot("screenshot")
                )
                logger.info(f"screenshot cost time{time.time() - time_s}")
            elif self.sandbox_type == "phone_wuyin":
                time_s = time.time()
                logger.info(
                    f"执行中analyse_screenshot_android_instance_name:"
                    f" {self.equipment.instance_manager.instance_id}",
                )
                screenshot_img, screenshot_oss, screenshot_filename = (
                    await self.take_screenshot("screenshot")
                )
                logger.info(f"screenshot cost time{time.time() - time_s}")
                width, height = await get_image_size_from_url(
                    screenshot_oss,
                )
                auxiliary_info["width"] = width
                auxiliary_info["height"] = height
            else:
                screenshot_img, screenshot_oss, screenshot_filename = (
                    await self.take_screenshot("screenshot")
                )

            # 发送AI分析阶段状态
            yield DataContent(
                data={
                    "step": f"{step_count}",
                    "stage": "ai_analysis",
                    "type": "analysis_stage",
                    "text": "analyzing",
                    "timestamp": time.time(),
                    "uuid": str(uuid4()),
                },
            )
        except Exception as e:
            yield DataContent(
                data={
                    "step": f"{step_count}",
                    "stage": "error",
                    "type": "SYSTEM",
                    "text": "Error taking screenshot: %s" % e,
                },
            )
            logger.error(f"Error taking screenshot: {e}")
            return

        if self.mode == "pc_use":
            try:
                # app v2
                messages = [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "data",
                                "data": {
                                    "messages": [
                                        {"image": screenshot_oss},
                                        {"instruction": self.chat_instruction},
                                        {"add_info": self.add_info},
                                        {"session_id": self.session_id},
                                        {"a11y": []},
                                        {"use_a11y": 0},
                                        {"use_reflection": False},
                                        {"task_is_complex": False},
                                        {"thought_language": "chinese"},
                                    ],
                                },
                            },
                        ],
                    },
                ]
                model_name = "pre-gui_owl_7b"
                if isinstance(self.e2e_info, list):
                    messages[0]["content"][0]["data"]["messages"].extend(
                        self.e2e_info,
                    )
                    for item in self.e2e_info:
                        if isinstance(item, dict) and "model_name" in item:
                            model_name = item["model_name"]
                            break
                elif (
                    self.e2e_info
                ):  # 如果e2e_info存在但不是列表，将其作为单个元素添加
                    messages[0]["content"][0]["data"]["messages"].append(
                        self.e2e_info,
                    )
                    model_name = self.e2e_info["model_name"]

                # 添加新的param_list字典
                param_dict = {
                    "param_list": [
                        {"add_info": self.add_info},
                        {"a11y": ""},
                        {"use_a11y": -1},
                        {"enable_reflector": True},
                        {"enable_notetaker": True},
                        {"worker_model": model_name},
                        {"manager_model": model_name},
                        {"reflector_model": model_name},
                        {"notetaker_model": model_name},
                    ],
                }

                # 将param_dict添加到messages中
                messages[0]["content"][0]["data"]["messages"].append(
                    param_dict,
                )
                mode_response = await gui_agent.run(messages, "pc_use")
                logger.info(f"pc模型返回：{mode_response}")
                # 发送图像处理阶段状态
                yield DataContent(
                    data={
                        "step": f"{step_count}",
                        "stage": "image_processing",
                        "type": "analysis_stage",
                        "message": "processing",
                        "timestamp": time.time(),
                        "uuid": str(uuid4()),
                    },
                )

                # 添加短暂延迟，确保前端能够处理image_processing状态
                await asyncio.sleep(0.2)

                action = mode_response.get("action", "")
                action_params = mode_response.get("action_params", {})

                self.session_id = mode_response.get("session_id", "")
                auxiliary_info["request_id"] = mode_response.get(
                    "request_id",
                    "",
                )
                auxiliary_info["session_id"] = mode_response.get(
                    "session_id",
                    "",
                )

                # 为click类型的动作生成标注图片
                if "position" in action_params:
                    try:
                        point_x = action_params["position"][0]
                        point_y = action_params["position"][1]
                        _, img_path = await self.annotate_image(
                            [point_x, point_y],
                            anno_type="point",
                            is_save=True,
                        )
                        auxiliary_info["annotated_img_path"] = img_path
                    except Exception as e:
                        logger.error(
                            f"Error generating annotated image: {e}",
                        )
                elif (
                    "position1" in action_params
                    and "position2" in action_params
                ):
                    _, img_path = await self.annotate_image(
                        [
                            action_params["position1"][0],
                            action_params["position1"][1],
                            action_params["position2"][0],
                            action_params["position2"][1],
                        ],
                        anno_type="box",
                        is_save=True,
                    )
                    auxiliary_info["annotated_img_path"] = img_path
                else:
                    # by cjj 所有操作都保留截图
                    auxiliary_info["annotated_img_path"] = screenshot_oss

                result_data = {
                    "thought": mode_response.get("thought", ""),
                    "action": action,
                    # "action_params": action_params,
                    "explanation": mode_response.get("explanation", ""),
                    "annotated_img_path": auxiliary_info.get(
                        "annotated_img_path",
                        "",
                    ),
                }
                result = json.dumps(result_data, ensure_ascii=False)

            except Exception as e:
                # 解析GUI服务错误，特别处理绿网拦截错误
                error_info = self._parse_gui_service_error(e)
                logger.error(f"Error querying PC use model: {e}")

                # 发送错误信息
                yield DataContent(
                    data={
                        "step": f"{step_count}",
                        "stage": "error",
                        "type": "SYSTEM",
                        "text": error_info["formatted_message"],
                    },
                )

                # 发送分析阶段失败状态，确保前端不会卡在AI分析阶段
                yield DataContent(
                    data={
                        "step": f"{step_count}",
                        "stage": "error",
                        "type": "analysis_stage",
                        "text": "Analysis failed",
                        "timestamp": time.time(),
                        "uuid": str(uuid4()),
                    },
                )

                # 如果是绿网拦截错误，抛出特殊异常以立即终止任务
                if error_info["is_green_net_error"]:
                    raise GreenNetInterceptionError(
                        error_info["formatted_message"],
                        request_id=error_info.get("request_id"),
                        error_code=error_info.get("error_code"),
                    )
                else:
                    raise RuntimeError(
                        f"GUI服务请求失败: {error_info['formatted_message']}",
                    )
        elif self.mode == "phone_use":
            try:
                messages = [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "data",
                                "data": {
                                    "messages": [
                                        {"image": screenshot_oss},
                                        {"instruction": self.chat_instruction},
                                        {"add_info": self.add_info},
                                        {"session_id": self.session_id},
                                        {"thought_language": "chinese"},
                                    ],
                                },
                            },
                        ],
                    },
                ]
                if isinstance(self.e2e_info, list):
                    messages[0]["content"][0]["data"]["messages"].extend(
                        self.e2e_info,
                    )
                elif (
                    self.e2e_info
                ):  # 如果e2e_info存在但不是列表，将其作为单个元素添加
                    messages[0]["content"][0]["data"]["messages"].append(
                        self.e2e_info,
                    )

                # 添加新的param_list字典
                param_dict = {
                    "param_list": [
                        {"add_info": self.add_info},
                    ],
                }

                # 将param_dict添加到messages中
                messages[0]["content"][0]["data"]["messages"].append(
                    param_dict,
                )

                mode_response = await gui_agent.run(messages, "phone_use")
                action = mode_response.get("operation", "")
                logger.info(f"phone模型返回：{mode_response} - {action}")
                # 发送图像处理阶段状态
                yield DataContent(
                    data={
                        "step": f"{step_count}",
                        "stage": "image_processing",
                        "type": "analysis_stage",
                        "message": "processing",
                        "timestamp": time.time(),
                        "uuid": str(uuid4()),
                    },
                )

                # 添加短暂延迟，确保前端能够处理image_processing状态
                await asyncio.sleep(0.2)

                if "click" in action.lower():
                    # 为click类型的动作生成标注图片
                    try:
                        print(f"Received action: {action}")
                        coordinate = (
                            action.split("(")[-1].split(")")[0].split(",")
                        )
                        x1, y1, x2, y2 = (
                            int(coordinate[0]),
                            int(coordinate[1]),
                            int(coordinate[2]),
                            int(
                                coordinate[3],
                            ),
                        )
                        x, y = int((x1 + x2) / 2), int((y1 + y2) / 2)
                        # 从auxiliary_info获取屏幕尺寸
                        width = auxiliary_info.get("width", 1080)
                        height = auxiliary_info.get("height", 1920)
                        point_x = int(x / 1000 * width)
                        point_y = int(y / 1000 * height)
                        _, img_path = await self.annotate_image(
                            [point_x, point_y],
                            anno_type="point",
                            is_save=True,
                        )
                        auxiliary_info["annotated_img_path"] = img_path

                    except Exception as e:
                        logger.info(
                            f"Error generating annotated image: {e}",
                        )
                else:
                    action = action
                    # 所有动作都要保存图片，但是只有要标记的才oss by cjj
                    auxiliary_info["annotated_img_path"] = screenshot_oss

                result_data = {
                    "thought": mode_response.get("thought", ""),
                    "action": action,
                    "explanation": mode_response.get("explanation", ""),
                    "annotated_img_path": auxiliary_info.get(
                        "annotated_img_path",
                        "",
                    ),
                }
                # 如果action包含括号，需要拆分
                if (
                    action
                    and isinstance(action, str)
                    and "(" in action
                    and ")" in action
                ):
                    # 提取括号前的部分作为action
                    action_part = action.split("(", 1)[0].strip()
                    # 提取括号及内部内容作为action_params

                    result_data["action"] = action_part
                    # result_data["action_params"] = params_part
                result = json.dumps(result_data, ensure_ascii=False)
                self.session_id = mode_response.get("session_id", "")
                auxiliary_info["request_id"] = mode_response.get(
                    "request_id",
                    "",
                )
                auxiliary_info["session_id"] = mode_response.get(
                    "session_id",
                    "",
                )
            except Exception as e:
                # 解析GUI服务错误，特别处理绿网拦截错误
                error_info = self._parse_gui_service_error(e)
                logger.error(f"Error querying Phone use model: {e}")

                # 发送错误信息
                yield DataContent(
                    data={
                        "step": f"{step_count}",
                        "stage": "error",
                        "type": "SYSTEM",
                        "text": error_info["formatted_message"],
                    },
                )

                # 发送分析阶段失败状态，确保前端不会卡在AI分析阶段
                yield DataContent(
                    data={
                        "step": f"{step_count}",
                        "stage": "error",
                        "type": "analysis_stage",
                        "text": "Analysis failed",
                        "timestamp": time.time(),
                        "uuid": str(uuid4()),
                    },
                )

                # 如果是绿网拦截错误，抛出特殊异常以立即终止任务
                if error_info["is_green_net_error"]:
                    raise GreenNetInterceptionError(
                        error_info["formatted_message"],
                        request_id=error_info.get("request_id"),
                        error_code=error_info.get("error_code"),
                    )
                else:
                    raise RuntimeError(
                        f"GUI服务请求失败: {error_info['formatted_message']}",
                    )
        else:
            logger.error(
                f"Invalid mode: {self.mode},"
                "must be one of: pc_use，phone_use",
            )
            raise ValueError(
                f"Invalid mode: {self.mode},"
                "must be one of: pc_use，phone_use",
            )

        # 发送完成状态之前添加短暂延迟，确保前端能够处理image_processing状态
        await asyncio.sleep(0.1)

        # 发送完成状态
        yield DataContent(
            data={
                "step": f"{step_count}",
                "stage": "completed",
                "type": "analysis_stage",
                "text": "completed",
                "timestamp": time.time(),
                "uuid": str(uuid4()),
            },
        )
        # Yield final result
        yield DataContent(
            data={
                "step": f"{step_count}",
                "stage": "completed",
                "type": "analysis_result",
                "text": result,
                "auxiliary_info": auxiliary_info,
                "mode_response": mode_response,
            },
        )

    async def _execute_pc_action(
        self,
        mode_response,
        equipment,
        step_count=None,
    ):
        """Execute PC actions based on mode response"""
        try:
            logger.info("Executing PC action")
            action_type = mode_response.get("action", "")
            action_parameter = mode_response.get("action_params", {})
            if action_type == "stop":
                print("stop")
                return {"result": "stop"}
            elif action_type == "open app":
                name = action_parameter["name"]
                if name == "File Explorer":
                    name = "文件资源管理器"
                await equipment.open_app_async(name)
            elif action_type == "wait":
                wait_time = action_parameter.get("time", 5)
                await asyncio.sleep(wait_time)
            elif action_type == "click":
                x = action_parameter["position"][0]
                y = action_parameter["position"][1]
                count = action_parameter["count"]
                await equipment.tap_async(x, y, count=count)
            elif action_type == "right click":
                x = action_parameter["position"][0]
                y = action_parameter["position"][1]
                count = action_parameter["count"]
                await equipment.right_tap_async(x, y, count=count)
            elif action_type == "hotkey":
                keylist = action_parameter["key_list"]
                await equipment.hotkey_async(keylist)
            elif action_type == "presskey":
                key = action_parameter["key"]
                await equipment.press_key_async(key)
            elif action_type == "click_type":
                x = action_parameter["position"][0]
                y = action_parameter["position"][1]
                text = action_parameter["text"]
                await equipment.tap_type_enter_async(x, y, text)
            elif action_type == "drag":
                x1 = action_parameter["position1"][0]
                y1 = action_parameter["position1"][1]
                x2 = action_parameter["position2"][0]
                y2 = action_parameter["position2"][1]
                await equipment.drag_async(x1, y1, x2, y2)
            elif action_type == "replace":
                x = action_parameter["position"][0]
                y = action_parameter["position"][1]
                text = action_parameter["text"]
                await equipment.replace_async(x, y, text)
            elif action_type == "append":
                x = action_parameter["position"][0]
                y = action_parameter["position"][1]
                text = action_parameter["text"]
                await equipment.append_async(x, y, text)
            elif action_type == "tell":
                answer_dict = action_parameter["answer"]
                print(answer_dict)
            elif action_type == "mouse_move":
                x = action_parameter["position"][0]
                y = action_parameter["position"][1]
                await equipment.mouse_move_async(x, y)
            elif action_type == "middle_click":
                x = action_parameter["position"][0]
                y = action_parameter["position"][1]
                await equipment.middle_click_async(x, y)
            elif action_type == "type_with_clear_enter":
                clear = action_parameter["clear"]
                enter = action_parameter["enter"]
                text = action_parameter["text"]
                await equipment.type_with_clear_enter_async(text, clear, enter)
            elif action_type == "call_user":
                task = mode_response.get("explanation")
                return await self._handle_human_intervention(task, step_count)
            elif action_type == "scroll":
                if "position" in action_parameter:  # -E
                    x = action_parameter["position"][0]
                    y = action_parameter["position"][1]
                    pixels = action_parameter["pixels"]
                    await equipment.scroll_pos_async(x, y, pixels)
                else:  # e2e
                    pixels = action_parameter["pixels"]
                    await equipment.scroll_async(pixels)
            elif action_type == "type_with_clear_enter_pos":  # New
                clear = action_parameter["clear"]
                enter = action_parameter["enter"]
                text = action_parameter["text"]
                x = action_parameter["position"][0]
                y = action_parameter["position"][1]
                await equipment.type_with_clear_enter_pos_async(
                    text,
                    x,
                    y,
                    clear,
                    enter,
                )
            else:
                logger.warning(f"Unknown action_type '{action_type}'")
                print(f"Warning: Unknown action_type '{action_type}'")

            return {"result": "continue"}

        except Exception as e:
            return self._handle_action_error(e, "_execute_pc_action")

    async def _execute_phone_action(
        self,
        mode_response,
        equipment,
        auxiliary_info,
        step_count=None,
    ):
        """Execute phone actions based on mode response"""
        try:
            action = mode_response.get("operation")
            operation_str_list = action.split("$")
            screen_size = 1
            width, height = auxiliary_info["width"], auxiliary_info["height"]
            for id_, operation in enumerate(operation_str_list):
                if "Select" in operation:
                    task = mode_response.get("explanation")
                    return await self._handle_human_intervention(
                        task,
                        step_count,
                    )
                elif "Click" in operation:
                    coordinate = (
                        operation.split("(")[-1].split(")")[0].split(",")
                    )
                    x1, y1, x2, y2 = (
                        int(coordinate[0]),
                        int(coordinate[1]),
                        int(coordinate[2]),
                        int(coordinate[3]),
                    )
                    await equipment.tab_async(x1, y1, x2, y2, width, height)
                elif "Swipe down" in operation:
                    x1, y1 = int(width * screen_size / 2), int(
                        height * screen_size / 3,
                    )
                    x2, y2 = int(width * screen_size / 2), int(
                        2 * height * screen_size / 3,
                    )
                    await equipment.slide_async(x1, y1, x2, y2)
                elif "Swipe up" in operation:
                    x1, y1 = int(width * screen_size / 2), int(
                        2 * height * screen_size / 3,
                    )
                    x2, y2 = int(width * screen_size / 2), int(
                        height * screen_size / 3,
                    )
                    await equipment.slide_async(x1, y1, x2, y2)
                elif "Swipe" in operation:
                    coordinate = (
                        operation.split("(")[-1].split(")")[0].split(",")
                    )
                    x1, y1, x2, y2 = (
                        int(int(coordinate[0]) / 1000 * width),
                        int(int(coordinate[1]) / 1000 * height),
                        int(int(coordinate[2]) / 1000 * width),
                        int(int(coordinate[3]) / 1000 * height),
                    )
                    a_x1, a_x2 = int(x1 * screen_size + x2 * 0), int(
                        x1 * 0 + x2 * screen_size,
                    )
                    a_y1, a_y2 = int(y1 * screen_size + y2 * 0), int(
                        y1 * 0 + y2 * screen_size,
                    )
                    await equipment.slide_async(a_x1, a_y1, a_x2, a_y2)
                elif "Type" in operation:
                    parameter = operation.split("(")[-1].split(")")[0]
                    await equipment.type_async(parameter)
                elif "Back" in operation:
                    await equipment.back_async()
                elif "Home" in operation:
                    await equipment.home_async()
                elif "Done" in operation:
                    return {"result": "stop"}
                elif "Answer" in operation:
                    return {"result": operation}
                elif "Wait" in operation:
                    task = mode_response.get("explanation")
                    return await self._handle_human_intervention(
                        task,
                        step_count,
                    )
                else:
                    print(f"Warning: Unknown phone operation '{operation}'")

            return {"result": "continue"}

        except Exception as e:
            return self._handle_action_error(e, "_execute_phone_action")
