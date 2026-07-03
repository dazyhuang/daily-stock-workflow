# 每日选股工作流

一个面向 A 股研究的实验性选股工作流，覆盖市场信息收集、候选池构建、LLM 多因子评分、回测验证、盘中执行辅助和周度复盘。

这是经过清理的公开版本。仓库不包含真实运行报告、日志、账户状态、券商导出文件、API Key、Webhook 地址或本机路径。

## 功能概览

- Phase 1：收集市场、新闻、技术面、基本面和情绪信息。
- Phase 2：构建候选股票池，并使用 LLM 辅助辩论和多因子评分。
- Phase 3：对 Top 候选股做回测验证，再生成操作建议。
- Phase 4：提供可选的盘中买入时机、持仓监控和通知推送辅助。
- 复盘：支持 Top5 归因复盘、周度总结和参数更新。

## 安全默认值

公开版本建议先以 dry-run 方式运行。真实交易需要你自行配置本地行情/交易桥接服务、券商环境、模型服务密钥，并完成独立验证。

本项目不构成投资建议，仅用于研究和工程实验。

## 安装

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

然后在 `.env` 中填入你自己的模型、数据源、通知和本地行情桥接配置。不要提交 `.env`。

## 常用命令

```bash
# 完整每日工作流。配置外部数据源和 LLM 后会调用相关服务。
python3 workflow.py

# 带锁和 watchdog 的稳定运行入口。
python3 run_daily_stock_workflow_stable.py

# 盘中买入时机辅助。
python3 intraday_executor.py --mode=buy-timing

# 持仓监控。
python3 intraday_executor.py --mode=monitor

# 查看当前状态。
python3 intraday_executor.py --mode=status

# 检查最近 N 天资金流质量。
python3 check_money_flow_quality.py --days 10

# Top5 归因复盘。
python3 top5_review_attribution.py --days 10
```

## 配置说明

主要环境变量见 `.env.example`。

- `DRY_RUN=1`：推荐的首次运行模式。
- `MX_APIKEY`、`MINIMAX_API_KEY`、`MX_DIRECT_KEY`、`VOLCAN_API_KEY`、`VOLCAN_ENGINE_API_KEY`：按你启用的模型和数据链路配置。
- `FEISHU_WEBHOOK_URL`：配置后启用飞书通知推送。
- `QMT_HTTP_URL`、`XQSHARE_HTTP_BASE`：指向你的本地行情/交易桥接服务。公开默认值使用 `127.0.0.1` 占位。

## 运行产物

以下文件只应保留在本地，已通过 `.gitignore` 排除：

- `logs/`
- `output/`
- `runtime_archive/`
- `knowledge-base/*.json`
- `weekly_strategy/checkpoints/*.json`

## 开源前清理范围

公开仓库已移除或替换：

- `.env` 和真实 API Key
- 飞书 Webhook
- 真实交易、持仓和执行记录
- 历史运行报告、日志和缓存
- 本机绝对路径和内网地址
- Python 编译缓存和虚拟环境

## 许可证

MIT
