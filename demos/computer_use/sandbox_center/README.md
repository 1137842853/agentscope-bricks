# 沙箱环境组件 (Sandbox Center)

本目录包含沙箱环境相关组件，提供安全的代码执行、GUI工具和隔离运行环境。

## 📋 组件列表

### GUI Tools - 图形界面工具组件
提供图形用户界面相关的工具和功能。

**前置使用条件：**
- 支持图形界面的运行环境
- 必要的图形库依赖
- 适当的显示设备或虚拟显示

**主要功能：**
- GUI应用程序的沙箱运行
- 图形界面元素的自动化操作
- 截图和屏幕录制功能
- 窗口管理和控制

## 🔧 沙箱环境特点

### 安全隔离
- **进程隔离**: 每个任务在独立进程中运行
- **文件系统隔离**: 限制文件访问范围
- **网络隔离**: 可控制的网络访问权限
- **资源限制**: CPU、内存、磁盘使用限制

### 运行环境
- **多语言支持**: 支持Python、JavaScript、Java等
- **预装库**: 常用开发库和工具预装
- **版本管理**: 支持多版本运行环境
- **快速启动**: 容器化技术实现快速环境启动

## 🛡️ 安全机制

### 访问控制
- 文件系统访问权限严格控制
- 网络访问白名单机制
- 系统调用监控和限制
- 恶意代码检测和拦截

### 资源监控
- 实时监控CPU和内存使用
- 磁盘空间使用限制
- 网络流量监控
- 执行时间限制

## 🔧 环境变量配置

| 环境变量 | 必需 | 默认值 | 说明 |
|---------|------|--------|------|
| `SANDBOX_TIMEOUT` | ❌ | 30 | 沙箱执行超时时间（秒） |
| `SANDBOX_MEMORY_LIMIT` | ❌ | 512MB | 内存使用限制 |
| `SANDBOX_CPU_LIMIT` | ❌ | 1.0 | CPU使用限制（核心数） |
| `SANDBOX_NETWORK_ACCESS` | ❌ | false | 是否允许网络访问 |
| `GUI_DISPLAY_MODE` | ❌ | virtual | 图形显示模式 |

## 🚀 使用示例

### GUI工具使用示例
```python
from agentbricks.components.sandbox_center import GUITools
import asyncio

# 初始化GUI工具
gui_tools = GUITools()

async def gui_example():
    # 在沙箱环境中运行GUI应用
    result = await gui_tools.run_gui_app({
        "app_path": "/path/to/gui_app.py",
        "display_mode": "virtual",
        "timeout": 60
    })

    print("GUI应用运行结果:", result.status)
    if result.screenshot:
        print("截图保存路径:", result.screenshot)

# 自动化GUI操作示例
async def gui_automation_example():
    # 启动应用并执行自动化操作
    automation_result = await gui_tools.automate_gui({
        "actions": [
            {"type": "click", "coordinates": (100, 200)},
            {"type": "type", "text": "Hello World"},
            {"type": "key_press", "key": "Enter"}
        ],
        "capture_steps": True
    })

    print("自动化操作完成:", automation_result.success)

asyncio.run(gui_example())
```

### 沙箱代码执行示例
```python
# 安全代码执行
async def sandbox_execution_example():
    code_executor = SandboxExecutor()

    # Python代码执行
    python_result = await code_executor.execute({
        "language": "python",
        "code": """
import matplotlib.pyplot as plt
import numpy as np

x = np.linspace(0, 10, 100)
y = np.sin(x)

plt.figure(figsize=(10, 6))
plt.plot(x, y)
plt.title('Sine Wave')
plt.savefig('output.png')
plt.show()
        """,
        "timeout": 30,
        "allow_gui": True
    })

    print("代码执行状态:", python_result.status)
    print("输出结果:", python_result.output)
    if python_result.files:
        print("生成的文件:", python_result.files)

asyncio.run(sandbox_execution_example())
```

### 多环境支持示例
```python
# 多编程语言环境支持
async def multi_language_example():
    # JavaScript代码执行
    js_result = await code_executor.execute({
        "language": "javascript",
        "code": """
const canvas = require('canvas');
const { createCanvas, loadImage } = canvas;

const width = 800;
const height = 600;
const canvas_obj = createCanvas(width, height);
const ctx = canvas_obj.getContext('2d');

// 绘制图形
ctx.fillStyle = 'blue';
ctx.fillRect(0, 0, width, height);

// 保存图片
const fs = require('fs');
const buffer = canvas_obj.toBuffer('image/png');
fs.writeFileSync('canvas_output.png', buffer);

console.log('Canvas rendering completed');
        """,
        "timeout": 20
    })

    print("JavaScript执行结果:", js_result.output)

asyncio.run(multi_language_example())
```

## 🏗️ 架构设计

### 容器化架构
- **Docker容器**: 基于Docker的轻量级容器
- **镜像管理**: 预构建的语言环境镜像
- **动态扩缩**: 根据负载动态调整容器数量
- **资源调度**: 智能的资源分配和调度

### 安全层次
1. **系统级**: 操作系统级别的隔离和限制
2. **容器级**: 容器级别的资源控制
3. **应用级**: 应用级别的访问控制
4. **代码级**: 代码执行的安全检查

## 🎯 应用场景

### 代码教育
- 在线编程学习平台
- 代码示例演示和执行
- 编程作业自动评测
- 代码安全性教学

### 数据分析
- 安全的数据处理和分析
- 数据可视化生成
- 机器学习模型训练
- 统计分析报告生成

### 原型开发
- 快速原型验证
- GUI应用程序测试
- 跨平台兼容性测试
- 性能基准测试

## 📦 依赖包
- `docker`: Docker容器管理
- `psutil`: 系统资源监控
- `PIL/Pillow`: 图像处理
- `selenium`: Web自动化（可选）
- `pyautogui`: GUI自动化
- `asyncio`: 异步编程支持

## ⚠️ 使用注意事项

### 安全考虑
- 严格限制沙箱环境的文件访问权限
- 监控和记录所有沙箱活动
- 定期更新沙箱环境和安全补丁
- 实现代码执行的恶意行为检测

### 性能优化
- 合理设置资源限制，避免资源耗尽
- 使用容器缓存加速环境启动
- 实现沙箱环境的复用机制
- 监控系统负载，防止过载

### 运维管理
- 定期清理临时文件和容器
- 监控沙箱环境的健康状态
- 建立完善的日志记录机制
- 实现异常情况的自动恢复

## 🔗 相关组件
- 可与代码生成组件结合，提供安全的代码执行环境
- 支持与教育平台集成，构建在线编程学习系统
- 可与数据分析组件配合，提供隔离的分析环境
- 支持与测试框架集成，进行自动化测试