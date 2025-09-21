# 搜索组件 (Searches)

本目录包含各种搜索服务组件，提供智能搜索、信息检索和内容发现功能。

## 📋 组件列表

### 1. ModelstudioSearch - 百炼搜索组件
核心的智能搜索服务，支持多种搜索策略和信息源。

**前置使用条件：**
- 有效的DashScope API密钥， **该组件目前属于要邀测阶段，请联系开发者，并提供DASHSCOPE_API_KEY**
- 配置搜索服务策略
- 网络连接稳定

**输入参数 (SearchInput)：**
- `messages` (List): 搜索相关的对话消息
- `search_options` (Dict): 搜索选项配置
  - `search_strategy`: 搜索策略（web、news、academic等）
  - `max_results`: 最大搜索结果数量
  - `time_range`: 时间范围限制
  - `language`: 搜索语言
  - `region`: 地理区域限制
- `search_output_rules` (Dict): 输出格式规则
- `search_timeout` (int): 搜索超时时间
- `type` (str): 搜索类型

**输出参数 (SearchOutput)：**
- `search_result` (str): 搜索结果摘要
- `search_info` (Dict): 详细搜索信息
  - `sources`: 信息来源列表
  - `relevance_score`: 相关性评分
  - `search_time`: 搜索用时
  - `result_count`: 结果数量

**核心功能：**
- **智能搜索**: 基于语义理解的智能搜索
- **多源整合**: 整合多个信息源的搜索结果
- **实时搜索**: 获取最新的实时信息
- **结果过滤**: 基于相关性和质量的结果筛选
- **搜索优化**: 自动优化搜索查询和策略

### 2. ModelstudioSearchLite - 百炼搜索轻量版
提供轻量化的搜索功能，适合快速查询和资源受限场景。

**前置使用条件：**
- 前往 [百炼搜索](https://bailian.console.aliyun.com/?tab=mcp#/mcp-market/detail/WebSearch) 进行开通
- 较低的网络延迟要求

**主要特点：**
- 更快的响应速度
- 较低的资源消耗
- 简化的搜索选项
- 适合移动端应用

## 🔧 环境变量配置

| 环境变量 | 必需 | 默认值 | 说明 |
|---------|------|--------|------|
| `DASHSCOPE_API_KEY` | ✅ | - | DashScope API密钥 |
| `SEARCH_DEFAULT_STRATEGY` | ❌ | web | 默认搜索策略 |
| `SEARCH_MAX_RESULTS` | ❌ | 10 | 默认最大搜索结果数 |
| `SEARCH_TIMEOUT` | ❌ | 30 | 搜索超时时间（秒） |
| `SEARCH_ENABLE_CACHE` | ❌ | true | 是否启用搜索缓存 |
| `SEARCH_REGION` | ❌ | global | 默认搜索区域 |

## 🚀 使用示例

### 基础搜索示例

```python
from agentscope_bricks.components.searches import ModelstudioSearch
import asyncio

# 初始化搜索组件
search = ModelstudioSearch()


async def basic_search_example():
    result = await search.arun({
        "messages": [
            {"role": "user", "content": "最新的人工智能发展趋势"}
        ],
        "search_options": {
            "search_strategy": "news",
            "max_results": 5,
            "time_range": "last_month",
            "language": "zh-CN"
        },
        "search_timeout": 20
    })

    print("搜索结果摘要:", result.search_result)
    print("信息来源:", result.search_info["sources"])


asyncio.run(basic_search_example())
```

### 多策略搜索示例
```python
async def multi_strategy_search_example():
    # 学术搜索
    academic_result = await search.arun({
        "messages": [
            {"role": "user", "content": "深度学习在医疗诊断中的应用"}
        ],
        "search_options": {
            "search_strategy": "academic",
            "max_results": 10,
            "language": "en"
        }
    })

    # 新闻搜索
    news_result = await search.arun({
        "messages": [
            {"role": "user", "content": "人工智能最新政策"}
        ],
        "search_options": {
            "search_strategy": "news",
            "time_range": "last_week",
            "region": "china"
        }
    })

    print("学术搜索结果:", academic_result.search_result)
    print("新闻搜索结果:", news_result.search_result)

asyncio.run(multi_strategy_search_example())
```

### 高级搜索配置示例
```python
async def advanced_search_example():
    result = await search.arun({
        "messages": [
            {"role": "user", "content": "比较不同机器学习算法的性能"},
            {"role": "assistant", "content": "我来为您搜索相关的对比信息"},
            {"role": "user", "content": "重点关注准确率和效率"}
        ],
        "search_options": {
            "search_strategy": "comprehensive",
            "max_results": 15,
            "filters": {
                "content_type": ["article", "paper", "report"],
                "quality_threshold": 0.8,
                "exclude_domains": ["low-quality-site.com"]
            },
            "ranking_criteria": ["relevance", "authority", "freshness"]
        },
        "search_output_rules": {
            "include_citations": True,
            "summarize_results": True,
            "highlight_key_points": True
        }
    })

    print("综合搜索结果:", result.search_result)
    print("搜索统计:", result.search_info)

asyncio.run(advanced_search_example())
```

## 🔍 支持的搜索策略

### Web搜索
- **通用网页搜索**: 搜索整个互联网的网页内容
- **实时搜索**: 获取最新发布的网页信息
- **深度搜索**: 多层次的内容挖掘和分析

### 专业搜索
- **学术搜索**: 搜索学术论文、研究报告
- **新闻搜索**: 搜索新闻报道、时事信息
- **技术搜索**: 搜索技术文档、API参考
- **产品搜索**: 搜索产品信息、用户评价

### 多媒体搜索
- **图片搜索**: 基于描述搜索相关图片
- **视频搜索**: 搜索相关视频内容
- **文档搜索**: 搜索PDF、Word等文档

## 🏗️ 搜索架构

### 查询处理
1. **查询理解**: 分析用户查询意图和关键信息
2. **查询扩展**: 添加同义词、相关词汇
3. **查询优化**: 优化搜索查询以提高准确性
4. **多策略路由**: 根据查询类型选择最佳搜索策略

### 结果处理
1. **结果聚合**: 整合多个搜索源的结果
2. **去重处理**: 删除重复和相似的结果
3. **质量评估**: 评估结果的质量和可信度
4. **相关性排序**: 按相关性对结果进行排序
5. **内容摘要**: 生成结果摘要和关键点

### 缓存机制
- **查询缓存**: 缓存常见查询的结果
- **结果缓存**: 缓存高质量的搜索结果
- **智能更新**: 根据内容时效性自动更新缓存

## 📊 搜索优化

### 性能优化
- **并行搜索**: 同时查询多个信息源
- **结果预取**: 预取可能相关的搜索结果
- **智能缓存**: 基于用户行为的智能缓存策略
- **负载均衡**: 分散搜索请求到不同服务节点

### 质量控制
- **来源可信度评估**: 评估信息来源的可信度
- **内容质量检查**: 检查内容的准确性和完整性
- **时效性验证**: 验证信息的时效性
- **偏见检测**: 检测和标记可能存在偏见的内容

## 📦 依赖包
- `aiohttp`: 异步HTTP客户端
- `dashscope`: DashScope SDK
- `beautifulsoup4`: HTML解析
- `lxml`: XML/HTML处理
- `nltk`: 自然语言处理（可选）
- `elasticsearch`: 搜索引擎（可选）

## ⚠️ 使用注意事项

### 搜索策略选择
- 根据查询类型选择合适的搜索策略
- 考虑结果的时效性需求
- 平衡搜索深度和响应速度
- 根据用户场景调整搜索参数

### 结果质量管理
- 设置合适的相关性阈值
- 验证搜索结果的准确性
- 处理搜索结果不足的情况
- 建立用户反馈机制改进搜索质量

### API使用限制
- 遵守搜索服务的调用频率限制
- 合理设置超时时间避免长时间等待
- 实现错误处理和重试机制
- 监控API使用量和成本

## 🔗 相关组件
- 可与RAG组件结合，提供检索增强生成
- 支持与意图识别组件集成，实现智能搜索路由
- 可与内存组件配合，记录用户搜索历史
- 支持与插件系统集成，扩展特定领域搜索