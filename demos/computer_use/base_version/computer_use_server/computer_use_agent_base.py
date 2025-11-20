# -*- coding: utf-8 -*-
import os
from PIL import Image
import json
import datetime
import asyncio
import threading
from agentscope_bricks.utils.grounding_utils import draw_point, encode_image
from cua_utils_base import logger, Message, parse_json, QwenProvider
from agentscope_runtime.sandbox.box.cloud_api.utils.oss_client import OSSClient
from agents.gui_agent_app_v2 import (
    GuiAgent,
)

TYPING_DELAY_MS = 12
TYPING_GROUP_SIZE = 50
HUMAN_HELP_ACTION = "human_help"
vision_model = QwenProvider("qwen-vl-max")
action_model = QwenProvider("qwen-max")
gui_agent = GuiAgent()


def safe_strip(value):
    """安全的字符串strip方法"""
    if value is None:
        return ""
    if not isinstance(value, str):
        return str(value)
    return value.strip()


def get_basic_tools():
    """获取基础工具 schema（用于模型调用）"""
    return {
        "stop": {
            "description": "Indicate that the task has been completed.",
            "params": {},
        },
        HUMAN_HELP_ACTION: {
            "description": (
                "Wait for the given amount of time for human to do the task."
            ),
            "params": {
                "time": {
                    "type": "integer",
                    "description": (
                        "The estimated time to do the task in seconds. "
                        "Estimate conservatively - it's better to estimate "
                        "less time and retry if needed."
                    ),
                },
                "task": {
                    "type": "string",
                    "description": (
                        "The task for human to do while the system is waiting."
                    ),
                },
            },
        },
        "click": {
            "description": (
                "Click at specific coordinates or based on visual query. "
                "If query is provided, it will search for the "
                "element visually."
            ),
            "params": {
                "x": {
                    "type": "integer",
                    "description": "X coordinate for clicking",
                },
                "y": {
                    "type": "integer",
                    "description": "Y coordinate for clicking",
                },
                "count": {
                    "type": "integer",
                    "description": "Number of clicks (1 for single click"
                    ", 2 for double click)",
                },
                "query": {
                    "type": "string",
                    "description": "Visual query to find the element to click",
                },
            },
        },
        "right_click": {
            "description": "Right click at specific coordinates.",
            "params": {
                "x": {
                    "type": "integer",
                    "description": "X coordinate for" " right clicking",
                },
                "y": {
                    "type": "integer",
                    "description": "Y coordinate for" " right clicking",
                },
            },
        },
        "type_text": {
            "description": "Type text in the sandbox environment.",
            "params": {
                "text": {"type": "string", "description": "The text to type"},
            },
        },
        "click_and_type": {
            "description": "Click at coordinates and then type text.",
            "params": {
                "x": {
                    "type": "integer",
                    "description": "X coordinate for clicking",
                },
                "y": {
                    "type": "integer",
                    "description": "Y coordinate for clicking",
                },
                "text": {
                    "type": "string",
                    "description": "The text to type" " after clicking",
                },
            },
        },
        "press_key": {
            "description": "Press a key or key combination.",
            "params": {
                "key": {
                    "type": "string",
                    "description": "Single key to press "
                    "(e.g., 'Enter', 'Tab')",
                },
                "key_combination": {
                    "type": "string",
                    "description": "Key combination to"
                    " press (e.g., 'Ctrl+C')",
                },
            },
        },
        "run_shell_command": {
            "description": "Execute a shell command in the sandbox.",
            "params": {
                "command": {
                    "type": "string",
                    "description": "The shell command" " to execute",
                },
                "background": {
                    "type": "boolean",
                    "description": "Whether to run the command in"
                    " the background",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Timeout for the command execution"
                    " in seconds",
                },
            },
        },
        "screenshot": {
            "description": "Take a screenshot and save it to a file.",
            "params": {
                "file_path": {
                    "type": "string",
                    "description": "Path where to save the screenshot",
                },
            },
        },
    }


class ComputerUseAgent:
    def __init__(
        self,
        equipment,
        output_dir=".",
        mode="qwen_vl",
        sandbox_type="e2b-desktop",
        save_logs=True,
        status_callback=None,
        pc_use_add_info: str = "",
        max_steps: int = 10,
    ):
        super().__init__()
        self.messages = []  # Agent memory
        # self.sandbox = sandbox  # E2B sandbox
        self.latest_screenshot = None  # Most recent PNG of the scren
        self.image_counter = 0  # Current screenshot number
        self.tmp_dir = output_dir  # Folder to store screenshots
        self.mode = mode
        self.sandbox_type = sandbox_type
        self.status_callback = status_callback  # 状态回调函数
        self.max_steps = max_steps
        self.equipment = equipment
        self.oss_client = OSSClient()
        # 修改设备处理逻辑
        if hasattr(equipment, "device") and equipment.device:
            self.sandbox = equipment.device
        else:
            # 如果equipment本身就是设备对象，则直接使用
            self.sandbox = equipment

        # 初始化工具（简化版本，仅用于模型调用）
        self.tools = {}
        try:
            if mode == "qwen_vl":
                self.tools = get_basic_tools()
            elif mode == "pc_use":
                self.session_id = ""
                self.add_info = pc_use_add_info
            else:
                raise ValueError(
                    f"Invalid mode: {mode}, must be one "
                    f"of: [qwen_vl, pc_use, wy_pc_use]",
                )
        except Exception as e:
            logger.log(f"Error initializing mode: {e}", "red")

        # Set the log file location
        if save_logs:
            logger.log_file = f"{output_dir}/log.html"

        self._is_cancelled = False
        self._interrupted = False

    def stop(self):
        self._is_cancelled = True
        print("Agent stopped by user request.")
        # 发送状态更新到前端
        self.emit_status(
            "SYSTEM",
            {
                "message": "Stop request received, "
                "waiting for current step to complete...",
                "status": "running",
            },
        )

    def interrupt_wait(self):
        """
        由前端调用，用于中断当前的等待状态
        """
        self._interrupted = True
        print("Agent wait stopped by user request.")
        # 发送状态更新到前端
        self.emit_status(
            "SYSTEM",
            {
                "message": "Stop wait request received, "
                "waiting for current step to complete...",
                "status": "running",
            },
        )

    def emit_status(self, status_type: str, data: dict):
        """发射状态更新 - 支持同步和异步回调"""
        status_data = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type": status_type,
            "status": "running",
            "data": data,
        }

        if self.status_callback:
            try:
                if asyncio.iscoroutinefunction(self.status_callback):
                    # 异步回调函数
                    self._run_async_callback(status_data)
                else:
                    # 同步回调函数
                    self.status_callback(status_data)
            except Exception as e:
                logger.log(f"Error in status callback: {e}", "red")

    def annotate_image(
        self,
        point: list,
        is_save: bool = False,
    ):
        annotated_img = draw_point(Image.open(self.latest_screenshot), point)
        screenshot_filename = os.path.basename(self.latest_screenshot)
        img_path = None
        if is_save:
            img_path = self.save_image(
                annotated_img,
                f"{screenshot_filename[:-4]}_annotated",
            )
        # 上传到oss
        oss_url = self.oss_client.oss_upload_file_and_sign(
            img_path,
            screenshot_filename,
        )
        return encode_image(annotated_img), oss_url

    def _run_async_callback(self, status_data):
        """在后台线程中运行异步回调"""

        def run_callback():
            try:
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self.status_callback(status_data))
                loop.close()
            except Exception as e:
                logger.log(f"Error running async callback: {e}", "red")

        # 在后台线程中运行
        thread = threading.Thread(target=run_callback)
        thread.daemon = True
        thread.start()

    def _execute_pc_action(self, mode_response, step_count=None):
        """
        Execute PC actions based on mode response.
        This method maps action_type to E2B tool calls.

        Args:
            mode_response: Dictionary or object containing
            'action' and 'action_params'
            step_count: Optional step count for logging

        Returns:
            Dict with 'result' key indicating 'stop', 'continue'
            , or error information
        """
        try:
            # 支持字典或对象格式的 mode_response
            if isinstance(mode_response, dict):
                action_type = mode_response.get("action", "")
                action_parameter = mode_response.get("action_params", {})
            else:
                action_type = getattr(mode_response, "action", "")
                action_parameter = getattr(mode_response, "action_params", {})

            if not action_type:
                logger.log("Warning: No action type provided", "yellow")
                return {
                    "result": "continue",
                    "error": "No action type provided",
                }

            logger.log(f"Executing PC action: {action_type}", "gray")

            # 处理 stop 动作
            if action_type == "stop":
                logger.log("Task stopped by action", "yellow")
                return {"result": "stop"}

            # 处理 human_help / call_user 动作
            if action_type in ["call_user", HUMAN_HELP_ACTION]:
                task = (
                    mode_response.get("explanation", "")
                    if isinstance(mode_response, dict)
                    else getattr(mode_response, "explanation", "")
                )
                # 如果没有 explanation，尝试从 action_params 中获取 task
                if not task and isinstance(action_parameter, dict):
                    task = action_parameter.get("task", "")
                return self._handle_human_intervention(task, step_count)

            # 映射 action_type 到 E2B 工具调用
            tool_name = None
            tool_arguments = {}

            if action_type == "click":
                # 映射 click 到 E2B click 工具
                if "position" in action_parameter:
                    tool_arguments["x"] = action_parameter["position"][0]
                    tool_arguments["y"] = action_parameter["position"][1]
                else:
                    tool_arguments["x"] = action_parameter.get("x", 0)
                    tool_arguments["y"] = action_parameter.get("y", 0)
                tool_arguments["count"] = action_parameter.get("count", 1)
                if "query" in action_parameter:
                    tool_arguments["query"] = action_parameter["query"]
                tool_name = "click"

            elif action_type == "right click":
                # 映射 right click 到 E2B right_click 工具
                if "position" in action_parameter:
                    tool_arguments["x"] = action_parameter["position"][0]
                    tool_arguments["y"] = action_parameter["position"][1]
                else:
                    tool_arguments["x"] = action_parameter.get("x", 0)
                    tool_arguments["y"] = action_parameter.get("y", 0)
                tool_name = "right_click"

            elif (
                action_type == "click_type" or action_type == "click_and_type"
            ):
                # 映射 click_type 到 E2B click_and_type 工具
                if "position" in action_parameter:
                    tool_arguments["x"] = action_parameter["position"][0]
                    tool_arguments["y"] = action_parameter["position"][1]
                else:
                    tool_arguments["x"] = action_parameter.get("x", 0)
                    tool_arguments["y"] = action_parameter.get("y", 0)
                tool_arguments["text"] = action_parameter.get("text", "")
                tool_name = "click_and_type"

            elif (
                action_type == "type_text"
                or action_type == "type"
                or action_type == "type_with_clear_enter_pos"
            ):
                # 映射 type_text 到 E2B type_text 工具
                tool_arguments["text"] = action_parameter.get("text", "")
                tool_name = "type_text"

            elif action_type == "presskey" or action_type == "press_key":
                # 映射 presskey 到 E2B press_key 工具
                if "key" in action_parameter:
                    tool_arguments["key"] = action_parameter["key"]
                if "key_combination" in action_parameter:
                    tool_arguments["key_combination"] = action_parameter[
                        "key_combination"
                    ]
                tool_name = "press_key"

            elif action_type == "hotkey":
                # 映射 hotkey 到 E2B press_key 工具（使用 key_combination）
                if "key_list" in action_parameter:
                    # 将 key_list 转换为 key_combination 格式，如 "Ctrl+C"
                    key_list = action_parameter["key_list"]
                    if isinstance(key_list, list):
                        tool_arguments["key_combination"] = "+".join(key_list)
                    else:
                        tool_arguments["key_combination"] = str(key_list)
                tool_name = "press_key"

            elif (
                action_type == "run_shell_command"
                or action_type == "run_command"
            ):
                # 映射 run_command 到 E2B run_shell_command 工具
                tool_arguments["command"] = action_parameter.get("command", "")
                tool_arguments["background"] = action_parameter.get(
                    "background",
                    False,
                )
                tool_arguments["timeout"] = action_parameter.get("timeout", 60)
                tool_name = "run_shell_command"

            elif action_type == "screenshot":
                # 映射 screenshot 到 E2B screenshot 工具
                if "file_path" not in action_parameter:
                    self.image_counter += 1
                    filename = f"screenshot_{self.image_counter}.png"
                    filepath = os.path.join(self.tmp_dir, filename)
                    tool_arguments["file_path"] = filepath
                else:
                    tool_arguments["file_path"] = action_parameter["file_path"]
                tool_name = "screenshot"

            elif action_type == "wait":
                # 处理 wait 动作（不需要调用 sandbox）
                wait_time = action_parameter.get("time", 5)
                import time

                time.sleep(wait_time)
                return {
                    "result": "continue",
                    "output": f"Waited for {wait_time} seconds",
                }

            else:
                logger.log(
                    f"Warning: Unknown action_type '{action_type}'",
                    "yellow",
                )
                return {
                    "result": "continue",
                    "error": f"Unknown action_type '{action_type}'",
                }

            # 调用 E2B 工具
            if tool_name:
                result = self.equipment._call_cloud_tool(
                    tool_name,
                    tool_arguments,
                )

                # 处理返回结果格式
                if isinstance(result, dict):
                    if result.get("success"):
                        output = result.get(
                            "output",
                            "Tool executed successfully",
                        )
                        # 如果是 screenshot，更新 latest_screenshot
                        if (
                            tool_name == "screenshot"
                            and "file_path" in tool_arguments
                        ):
                            self.latest_screenshot = tool_arguments[
                                "file_path"
                            ]
                        return {"result": "continue", "output": output}
                    else:
                        error_msg = result.get("error", "Unknown error")
                        return {
                            "result": "continue",
                            "error": error_msg,
                        }
                else:
                    return {"result": "continue", "output": str(result)}

            return {"result": "continue"}

        except Exception as e:
            logger.log(f"Error in _execute_pc_action: {e}", "red")
            return {
                "result": "continue",
                "error": f"Error executing action: {str(e)}",
            }

    def _handle_human_intervention(self, task, step_count=None):
        """
        Handle human intervention request.

        Args:
            task: Task description for human
            step_count: Optional step count

        Returns:
            Dict with result information, including status update info
        """
        import time

        time_to_sleep = int(os.getenv("HUMAN_WAIT_TIME", 15))
        logger.log(
            f"HUMAN_HELP: The system will wait for {time_to_sleep} "
            f"seconds for human to do the task: {task}",
        )

        # 构建状态更新信息
        status_info = {
            "human_help_status": False,
            "action_executed": (
                f"The system will wait for {time_to_sleep} "
                f"seconds for human to do the task:\n\n {task}"
            ),
        }

        # 可中断等待
        start_time = time.time()
        waited_time = 0
        sleep_interval = min(5, time_to_sleep)

        # 重置中断标志
        self._interrupted = False

        # 可中断的等待循环
        while waited_time < time_to_sleep and not self._interrupted:
            time.sleep(min(sleep_interval, time_to_sleep - waited_time))
            waited_time = time.time() - start_time

        if self._interrupted:
            logger.log("Human help wait was interrupted by user.", "yellow")
            self._interrupted = False
            status_info["human_help_status"] = False
            return {
                "result": "continue",
                "output": "Human help wait was interrupted",
                "status_info": status_info,
            }
        else:
            logger.log("Human help wait completed.", "yellow")
            status_info["human_help_status"] = True
            return {
                "result": "continue",
                "output": f"Human help wait completed for task: {task}",
                "status_info": status_info,
            }

    def call_function(self, name, arguments, step_info=None):
        """
        调用工具函数，通过 _execute_pc_action 方法执行。
        将工具名和参数转换为 action 格式，然后调用 _execute_pc_action。

        Args:
            name: 工具名称
            arguments: 工具参数
            step_info: 可选的步骤信息字典，用于更新状态

        Returns:
            执行结果的字符串表示
        """
        # 处理特殊工具 stop
        if name == "stop":
            return "Task stopped"

        # 确保 arguments 是字典类型
        if isinstance(arguments, str):
            arguments = parse_json(arguments) or {}
        elif arguments is None:
            arguments = {}

        # 处理传入的是 JSON Schema 格式的情况
        if isinstance(arguments, dict) and "properties" in arguments:
            # 提取实际的参数值
            arguments = arguments.get("properties", {})

        # 将工具名映射到 action_type
        # 工具名到 action_type 的映射
        tool_name_mapping = {
            "right_click": "right click",  # 需要转换为空格格式
            # 其他工具名可以直接使用，因为 _execute_pc_action 支持多种格式
        }
        action_type = tool_name_mapping.get(name, name)

        # 构建 mode_response 格式
        mode_response = {
            "action": action_type,
            "action_params": arguments,
        }

        # 处理 human_help 特殊情况
        if name == HUMAN_HELP_ACTION:
            mode_response["action"] = HUMAN_HELP_ACTION
            mode_response["explanation"] = arguments.get("task", "")

        try:
            # 调用 _execute_pc_action
            result = self._execute_pc_action(mode_response)

            # 处理状态更新（特别是 human_help 的状态）
            if step_info is not None and isinstance(result, dict):
                status_info = result.get("status_info", {})
                if status_info:
                    step_info.update(status_info)

            # 处理返回结果格式，将字典转换为字符串
            if isinstance(result, dict):
                if result.get("result") == "stop":
                    return "Task stopped"
                elif result.get("result") == "continue":
                    # 返回 output 或 error 信息
                    if "output" in result:
                        return result["output"]
                    elif "error" in result:
                        return f"Error: {result['error']}"
                    else:
                        return "Tool executed successfully"
                else:
                    return str(
                        result.get("output", "Tool executed successfully"),
                    )
            else:
                return str(result) if result else "Tool executed successfully"

        except Exception as e:
            logger.log(f"Error in call_function: {e}", "red")
            return (
                f"Error executing function: {str(e)}, "
                f"when calling function: {name} "
                f"with arguments: {arguments}"
            )

    def save_image(self, image, prefix="image"):
        self.image_counter += 1
        filename = f"{prefix}_{self.image_counter}.png"
        filepath = os.path.join(self.tmp_dir, filename)
        if isinstance(image, Image.Image):
            image.save(filepath)
        else:
            with open(filepath, "wb") as f:
                f.write(image)

        return filepath

    def screenshot(self):
        file = self.sandbox.screenshot()
        filename = self.save_image(file, "screenshot")
        logger.log(f"screenshot {filename}", "gray")
        self.latest_screenshot = filename
        with open(filename, "rb") as image_file:
            return image_file.read(), filename

    def screenshot_save_oss(self, data: bytes, file_name: str):
        return self.oss_client.oss_upload_data_and_sign(data, file_name)

    def analyse_screenshot(self, is_debug=False, debug_file_path=None):
        screenshot_img, screenshot_filename = self.screenshot()
        auxiliary_info = {}
        if self.mode == "qwen_vl":
            system_prompt = (
                "You are an intelligent computer-use "
                "agent that helps users "
                "accomplish tasks by interpreting desktop "
                "screenshots and "
                "generating the next UI action.\n\n"
                "For each screenshot, follow these steps "
                "and respond in the "
                "exact format below. Only use visual "
                "evidence — do not assume "
                "hidden or off-screen information.\n\n"
                f"### Objective"
                f":\n{getattr(self, 'user_instruction', 'Unknown')}\n\n"
                "### Response Format:\n```\n"
                "Screen analysis: [Describe relevant "
                "visible elements such as "
                "windows, apps, icons, buttons, menus]\n"
                "Objective status: [complete | not complete]\n"
                "(If the objective is not complete:)\n"
                "Next action: [click|type|run command] "
                "[describe the action "
                "clearly]\nExpected outcome: [What "
                "result do you expect this "
                "action to achieve?]\n```\n\n"
                "### Guidelines:\n"
                '* Be specific (e.g., "click the '
                'Chrome icon in the taskbar" '
                'not just "click Chrome").\n'
                "* Do **not** speculate about invisible UI.\n"
                "* Only suggest **one next action** at a time.\n"
                "* Use the screenshot to ground all decisions."
            )

            vl_messages = [
                Message(system_prompt, role="system"),
                Message(
                    [
                        screenshot_img,
                        "The image shows the current display of the computer.",
                    ],
                    role="user",
                ),
            ]

            # Debug: save vision_model request
            if is_debug and debug_file_path:
                with open(debug_file_path, "a", encoding="utf-8") as f:
                    f.write(f"\n{'=' * 50}\n")
                    f.write(
                        f"VISION_MODEL REQUEST - {datetime.datetime.now()}\n",
                    )
                    f.write(f"{'=' * 50}\n")
                    # Save the text content of messages (excluding image data)
                    for i, msg in enumerate(vl_messages):
                        role = msg.get("role", "user")
                        f.write(f"Message {i + 1} (role: {role}):\n")
                        content = msg.get("content", msg)
                        if isinstance(content, list):
                            for j, content_item in enumerate(content):
                                if isinstance(content_item, bytes):
                                    img_info = (
                                        f"[Screenshot saved as "
                                        f"{screenshot_filename}]"
                                    )
                                    f.write(
                                        f"  Image part {j + 1}: "
                                        f"{img_info}\n",
                                    )
                                elif isinstance(content_item, str):
                                    f.write(
                                        f"  Text part {j + 1}: "
                                        f"{content_item}\n",
                                    )
                                else:
                                    f.write(
                                        f"  Content part {j + 1}: "
                                        f"{str(content_item)}\n",
                                    )
                        else:
                            if content == screenshot_img:
                                img_info = (
                                    "[Screenshot saved as"
                                    f"{screenshot_filename}]"
                                )
                                f.write(f"  Content: {img_info}\n")
                            else:
                                f.write(f"  Content: {str(content)}\n")
                    f.write("\n")

            try:
                vision_result = vision_model.call(vl_messages)
                result = "THOUGHT: " + str(
                    (
                        vision_result
                        if vision_result
                        else "No response from vision model"
                    ),
                )
            except Exception as e:
                logger.log(f"Error calling vision model: {e}", "red")
                result = "THOUGHT: Error analyzing screenshot"

        elif self.mode == "pc_use":
            try:
                screenshot_oss_url = self.screenshot_save_oss(
                    screenshot_img,
                    screenshot_filename,
                )
                m_name = "pre-gui_owl_7b"
                messages = [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "data",
                                "data": {
                                    "messages": [
                                        {"image": screenshot_oss_url},
                                        {
                                            "instruction": getattr(
                                                self,
                                                "user_instruction",
                                                "Unknown task",
                                            ),
                                        },
                                        {"session_id": self.session_id},
                                        {
                                            "device_type": "pc",
                                        },
                                        {
                                            "pipeline_type": "agent",
                                        },
                                        {
                                            "model_name": m_name,
                                        },
                                        {"thought_language": "chinese"},
                                        {
                                            "param_list": [
                                                {"add_info": self.add_info},
                                                {"a11y": ""},
                                                {"use_a11y": -1},
                                                {"enable_reflector": True},
                                                {"enable_notetaker": True},
                                                {"worker_model": m_name},
                                                {"manager_model": m_name},
                                                {
                                                    "reflector_model": m_name,
                                                },
                                                {
                                                    "notetaker_model": m_name,
                                                },
                                            ],
                                        },
                                    ],
                                },
                            },
                        ],
                    },
                ]

                mode_response = asyncio.run(gui_agent.arun(messages, "pc_use"))

                action = (
                    mode_response.action
                    if hasattr(mode_response, "action")
                    else "unknown"
                )
                action_params = (
                    mode_response.action_params
                    if hasattr(mode_response, "action_params")
                    else {}
                )
                thought = (
                    mode_response.thought
                    if hasattr(mode_response, "thought")
                    else "No thought available"
                )

                result = (
                    "Thought: "
                    + str(thought)
                    + "\n\nAction: "
                    + str(action)
                    + "\n\nAction Params: "
                    + str(action_params)
                )

                if hasattr(mode_response, "session_id"):
                    self.session_id = mode_response.session_id
                if hasattr(mode_response, "request_id"):
                    auxiliary_info["request_id"] = mode_response.request_id

                # 保存 mode_response 到 auxiliary_info，供后续直接使用
                auxiliary_info["mode_response"] = mode_response.model_dump()

                # 为click类型的动作生成标注图片
                if action in ["click", "right click"]:
                    try:
                        if (
                            isinstance(action_params, dict)
                            and "position" in action_params
                        ):
                            point_x = action_params["position"][0]
                            point_y = action_params["position"][1]
                            _, img_path = self.annotate_image(
                                [point_x, point_y],
                                is_save=True,
                            )
                            auxiliary_info["annotated_img_path"] = img_path
                    except Exception as e:
                        logger.log(
                            f"Error generating annotated image: {e}",
                            "red",
                        )

            except Exception as e:
                logger.log(f"Error querying PC use model: {e}", "red")
                result = f"THOUGHT: Error querying PC use model: {e}"
        else:
            raise ValueError(
                f"Invalid mode: {self.mode},"
                "must be one of: [qwen_vl, pc_use]",
            )

        # Debug: save vision_model response
        if is_debug and debug_file_path:
            with open(debug_file_path, "a", encoding="utf-8") as f:
                f.write("VISION_MODEL RESPONSE:\n")
                f.write(f"{result}\n")
                f.write("=" * 50 + "\n\n")

        return result, auxiliary_info

    def run(self, instruction: str, is_debug=False):
        try:
            while not self._is_cancelled:
                self.messages.append(Message(f"OBJECTIVE: {instruction}"))
                self.user_instruction = instruction
                logger.log(f"USER: {instruction}", print=False)

                if self.mode == "pc_use":
                    self.session_id = ""

                # 发射任务开始状态
                self.emit_status(
                    "TASK",
                    {"message": "task=" + instruction + ", mode=" + self.mode},
                )

                # Setup debug file path if debug mode is enabled
                debug_file_path = None
                if is_debug:
                    debug_file_path = os.path.join(self.tmp_dir, "debug.txt")
                    # Create or clear the debug file
                    with open(debug_file_path, "w", encoding="utf-8") as f:
                        f.write(
                            f"DEBUG LOG - Started at "
                            f"{datetime.datetime.now()}\n",
                        )
                        f.write(f"OBJECTIVE: {instruction}\n")
                        f.write("=" * 80 + "\n\n")

                should_continue = True
                step_count = 0
                while should_continue and step_count < self.max_steps:
                    if self._is_cancelled:
                        break
                    step_count += 1
                    step_info = {
                        "step": step_count,
                        "auxiliary_info": {},
                        "observation": "",
                        "action_parsed": "",
                        "action_executed": "",
                    }
                    self.emit_status("STEP", step_info)

                    action_system_prompt = (
                        "You are an intelligent computer-use "
                        "agent that helps users "
                        "accomplish the objective. Every turn"
                        ", user will provide a "
                        "natural-language description of the "
                        "current screen and next "
                        "action to take. Your task is to use "
                        "tool calls to take "
                        "these actions, or use the stop command"
                        " if the objective is "
                        "complete. You are an assistant "
                        "that **must use tools** to "
                        "answer questions when possible. "
                        "Do not answer directly "
                        "unless no tools are available."
                    )

                    screenshot_analysis, auxiliary_info = (
                        self.analyse_screenshot(
                            is_debug,
                            debug_file_path,
                        )
                    )
                    step_info["observation"] = screenshot_analysis
                    if auxiliary_info:
                        step_info["auxiliary_info"].update(auxiliary_info)
                    self.emit_status("STEP", step_info)

                    # 根据模式决定如何处理动作
                    if self.mode == "pc_use":
                        # pc_use 模式：直接使用 analyse_screenshot 返回的 mode_response
                        mode_response = auxiliary_info.get("mode_response")
                        if mode_response:
                            # 记录 thought 到消息历史
                            thought = (
                                getattr(mode_response, "thought", "")
                                if hasattr(mode_response, "thought")
                                else ""
                            )
                            if thought:
                                self.messages.append(
                                    Message(
                                        logger.log(
                                            f"THOUGHT: {thought}",
                                            "blue",
                                        ),
                                    ),
                                )

                            # 记录 action 信息
                            action = (
                                getattr(mode_response, "action", "")
                                if hasattr(mode_response, "action")
                                else "unknown"
                            )
                            action_params = (
                                getattr(mode_response, "action_params", {})
                                if hasattr(mode_response, "action_params")
                                else {}
                            )

                            # 发射动作执行开始状态
                            step_info["action_parsed"] = (
                                f"Action: {action} Params:"
                                f" {str(action_params)}"
                            )
                            self.emit_status("STEP", step_info)

                            logger.log(
                                f"ACTION: {action} {str(action_params)}",
                                "red",
                            )

                            # 直接执行动作
                            action_result = self._execute_pc_action(
                                mode_response,
                                step_count,
                            )

                            # 处理返回结果
                            if isinstance(action_result, dict):
                                if action_result.get("result") == "stop":
                                    should_continue = False
                                    # 发射任务完成状态
                                    self.emit_status(
                                        "TASK",
                                        {
                                            "total_steps": step_count,
                                            "instruction": instruction,
                                        },
                                    )
                                    break

                                # 更新状态信息
                                status_info = action_result.get(
                                    "status_info",
                                    {},
                                )
                                if status_info:
                                    step_info.update(status_info)

                                # 如果 human_help 状态已更新，发送状态更新
                                if step_info.get("human_help_status") is True:
                                    self.emit_status("STEP", step_info)

                                # 记录执行结果
                                output = action_result.get(
                                    "output",
                                    "Action executed",
                                )
                                step_info["action_executed"] = output
                                self.emit_status("STEP", step_info)

                                # 添加到消息历史
                                self.messages.append(
                                    Message(
                                        logger.log(
                                            f"OBSERVATION: {output}",
                                            "yellow",
                                        ),
                                    ),
                                )
                            else:
                                should_continue = True
                        else:
                            logger.log(
                                "Warning: No mode_response in pc_use mode",
                                "yellow",
                            )
                            should_continue = False
                            break

                    elif self.mode == "qwen_vl":
                        # qwen_vl 模式：仍然需要模型来解析动作（保留原有逻辑）
                        action_messages = [
                            Message(action_system_prompt, role="system"),
                            *self.messages,
                            Message(
                                logger.log(
                                    f"{screenshot_analysis}",
                                    "green",
                                ),
                                role="user",
                            ),
                        ]

                        # Debug: save action_model request
                        if is_debug and debug_file_path:
                            with open(
                                debug_file_path,
                                "a",
                                encoding="utf-8",
                            ) as f:
                                f.write(f"\n{'=' * 50}\n")
                                f.write(
                                    f"ACTION_MODEL REQUEST - "
                                    f"{datetime.datetime.now()}\n",
                                )
                                f.write("=" * 50 + "\n")
                                for i, msg in enumerate(action_messages):
                                    role = msg.get("role", "user")
                                    f.write(
                                        f"Message {i + 1} (role: {role}):\n",
                                    )
                                    content = msg.get("content", msg)
                                    content_str = str(content)
                                    truncated = (
                                        content_str[:1000] + "..."
                                        if len(content_str) > 1000
                                        else content_str
                                    )
                                    f.write(f"  Content: {truncated}\n")

                        try:
                            content, tool_calls = action_model.call(
                                action_messages,
                                self.tools,
                            )
                        except Exception as e:
                            logger.log(
                                f"Error calling action model: {e}",
                                "red",
                            )
                            content = "Error calling action model"
                            tool_calls = [{"name": "stop", "parameters": {}}]

                        # Debug: save action_model response
                        if is_debug and debug_file_path:
                            with open(
                                debug_file_path,
                                "a",
                                encoding="utf-8",
                            ) as f:
                                f.write("ACTION_MODEL RESPONSE:\n")
                                f.write(f"Content: {content}\n")
                                f.write(f"Tool calls: {tool_calls}\n")
                                f.write("=" * 50 + "\n\n")

                        if content:
                            content_safe = (
                                str(content)
                                if content is not None
                                else "No content"
                            )
                            self.messages.append(
                                Message(
                                    logger.log(
                                        f"THOUGHT: {content_safe}",
                                        "blue",
                                    ),
                                ),
                            )

                        should_continue = False
                        for tool_call in tool_calls:
                            if self._is_cancelled:
                                break
                            name, parameters = tool_call.get(
                                "name",
                            ), tool_call.get(
                                "parameters",
                            )
                            should_continue = name != "stop"
                            if not should_continue:
                                # 发射任务完成状态
                                self.emit_status(
                                    "TASK",
                                    {
                                        "total_steps": step_count,
                                        "instruction": instruction,
                                    },
                                )
                                break

                            # 发射动作执行开始状态
                            step_info["action_parsed"] = (
                                f"Action: {name} Params: {str(parameters)}"
                            )

                            self.emit_status("STEP", step_info)

                            # Print the tool-call in an easily readable format
                            logger.log(
                                f"ACTION: {name} {str(parameters)}",
                                "red",
                            )
                            # format used by the model
                            self.messages.append(
                                Message(json.dumps(tool_call)),
                            )

                            # 初始化 human_help_status
                            step_info["human_help_status"] = False

                            try:
                                result = self.call_function(
                                    name,
                                    parameters,
                                    step_info,
                                )

                                # 如果 human_help 状态已更新，发送状态更新
                                if step_info.get("human_help_status") is True:
                                    self.emit_status("STEP", step_info)
                            except Exception as e:
                                result = f"Error executing function: {str(e)}"
                                logger.log(
                                    f"Error executing function:{e},{result}",
                                    "red",
                                )
                                continue

                            # 发射动作执行完成状态
                            step_info["action_executed"] = (
                                str(result)
                                if result is not None
                                else "No result"
                            )

                            self.emit_status("STEP", step_info)

                            result_safe = (
                                str(result)
                                if result is not None
                                else "No result"
                            )
                            self.messages.append(
                                Message(
                                    logger.log(
                                        f"OBSERVATION: {result_safe}",
                                        "yellow",
                                    ),
                                ),
                            )
                    else:
                        logger.log(f"Unknown mode: {self.mode}", "red")
                        should_continue = False
                        break
                if self._is_cancelled:
                    print("✅ Task canceled")
                    break
                elif not should_continue:
                    print("✅ Task completed")
                    break
                elif step_count >= self.max_steps:
                    print("✅ Task out max step, stop")
                    break

        except Exception as e:
            logger.log(f"Error in agent run: {e}", "red")
        finally:
            logger.log("Agent run loop exited.")
