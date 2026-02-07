# Opinion-Polymarket Alpha Monitor

Rust 实时监测与交易引擎：聚合 Opinion 与 Polymarket 的 WS/REST 数据，计算价差/冲击/跟随信号，输出监控指标并可选执行交易。

## 关键说明（必读）
本项目的 **市场匹配与发现** 依赖独立仓库 `https://github.com/uniteonline/opinion-polymarket-matcher` 生成的 `discovery.db`。本项目只负责读取该数据库并进行监控/信号/交易执行。

## 技术栈
- Rust 2021
- 异步运行时：`tokio`
- WebSocket：`tokio-tungstenite`
- HTTP：`reqwest`
- 序列化：`serde` / `serde_json` / `serde_yaml`
- 数据库：`sqlx`（PostgreSQL 为主，SQLite 用于队列缓存）
- 统计与指标：`hdrhistogram`
- 交易相关：`ethers-*`（签名/链上交互）
- 日志：`tracing` / `tracing-subscriber`

## 实现原理（高层）
1. **发现/匹配对齐**：从 `discovery.db` 读取 Opinion ↔ Polymarket 的市场配对（由外部 matcher 生成）。
2. **数据接入**：
   - Polymarket：WS 实时 + REST 快照补齐。
   - Opinion：WS 实时 + OpenAPI 快照校准与补齐。
3. **时间对齐与健康度**：对 WS/REST 时间戳做校准与降级策略，记录延迟与异常。
4. **秒级聚合**：为每个 token 维护 1s bar 与状态序列；跨交易所对齐到 pair 维度。
5. **信号/Alpha**：基于 shock/momentum/lag/edge 等指标输出交易信号。
6. **可选交易执行**：在 Opinion 侧 maker 为主的执行逻辑，具备风险阈值与撤单策略。
7. **持久化与审计**：将指标、信号、交易与异常写入 PostgreSQL，便于回放与分析。

## 目录结构
- `src/`：核心逻辑
  - `ws/`、`rest/`：数据接入
  - `pair_aggregator/`、`shock/`：信号/聚合
  - `trade/`：交易执行（可关闭）
  - `db/`、`db_writer/`：持久化
- `sql/`：PostgreSQL schema
- `analysis/`：分析脚本与 SQL
- `config.yaml`：主配置文件

## 安装
```bash
# Rust toolchain
rustup install stable

# Build
cargo build --release
```

## 配置
先复制模板（`config.yaml` 已加入 `.gitignore`，不会被提交）：
```bash
cp config.example.yaml config.yaml
```

默认读取 `./monitor/config.yaml`，在本仓库运行请显式指定：
```bash
export MONITOR_CONFIG=./config.yaml
```

必须配置（建议与 matcher 输出对齐）：
- `monitoring_db_url`：PostgreSQL 连接串
- `discovery_db_path`：matcher 输出的 `discovery.db`（来自 `opinion-polymarket-matcher`）
- `opinion_api_key`：Opinion OpenAPI key（也可用环境变量）

交易相关（如不交易可忽略）：
- `trade.enabled`：是否启用交易
- `trade.dry_run`：仅演练不下单
- `trade.opinion_account_file`：Opinion 账户文件路径

常用环境变量：
- `MONITOR_CONFIG`：配置文件路径
- `MONITOR_DB_URL`：覆盖 `monitoring_db_url`
- `DISCOVERY_DB_PATH`：覆盖 `discovery_db_path`
- `OPINION_API_KEY`：覆盖 `opinion_api_key`

## 使用
### 1) 先运行市场匹配（外部）
确保 matcher 生成 `discovery.db`，并在本项目 `config.yaml` 中指向该路径。

### 2) 启动监控/交易引擎
```bash
# 运行
cargo run --release
```

### 3) 仅监控、不交易
在 `config.yaml` 中设置：
```
trade:
  enabled: false
```
或使用：
```
trade:
  dry_run: true
```

## Alpha 监测与结果读取（重要）
### 如何开启 Alpha 监测（推荐：不下单）
1. 在 `config.yaml` 里设置 `alpha_capture_enabled: true`，用于记录 shock→Opinion 的跟随响应。
2. 在 `trade` 配置里设置 `enabled: true` 且 `dry_run: true`，保证 Alpha 计算与日志开启但不下单。
3. 设置 `trade.alpha_log_enabled: true`，并配置 `trade.alpha_log_jsonl_path` / `trade.alpha_log_csv_path` 到大容量磁盘。
4. 运行 `cargo run --release`，日志会开始写入。

### Alpha 日志在哪里
- JSONL：`trade.alpha_log_jsonl_path`（默认 `./records/alpha_log.jsonl`）
- CSV：`trade.alpha_log_csv_path`（默认 `./records/alpha_log.csv`）

**常看字段（日志里都有）**
- `phase`：ENTRY_SIGNAL / ENTRY_READY / ENTRY_PLACING / ENTRY_FILLED / POSITION_OPEN / EXIT...  
- `alpha_type`：ShockA / GapB / LagC（策略来源）  
- `alpha_value` / `alpha_min`：信号强度与阈值  
- `edge_raw` / `edge_norm`：Opinion 落后程度  
- `follow_score_raw` / `follow_score_adj`：跟随评分  
- `pm_move` / `pm_mom` / `ofi_250ms`：Polymarket 侧驱动指标  

### 如何读取 Alpha 结果（汇总/统计）
1. **SQL 报告（推荐）**  
执行内置脚本（依赖 PostgreSQL）：
```bash
psql "$MONITOR_DB_URL" -f analysis/alpha_follow.sql
psql "$MONITOR_DB_URL" -f analysis/follow_quality.sql
```

2. **Python 报告（可选）**  
```bash
python3 analysis/alpha_follow_report.py --db "$MONITOR_DB_URL"
```

这些报告主要依赖 `poly_shock_events`、`opinion_response`、`bars_1s_pair` 等表，可用于评估 shock→Opinion 跟随质量与 Alpha 有效性。

## 从监测切换到交易（务必确认）
当前配置默认使用 **`alpha_source: follow_score_raw`** 作为交易信号来源（可在 `config.yaml` 的 `trade.alpha_source` 修改为 `follow_score_adj` / `book_alpha_raw` / `book_alpha_adj`）。

**切换步骤：**
1. 先完成一段时间的监测与评估（见上面的 SQL/Python 报告），确认 Alpha 稳定有效。  
2. 修改 `config.yaml`：  
```
trade:
  enabled: true
  dry_run: false
  alpha_source: follow_score_raw   # 或你验证过的其他来源
```
3. 配置交易账户与权限：  
```
trade:
  opinion_account_file: ./config/accounts.json
  opinion_account_id: <your_account_id>
  opinion_enable_trading_on_startup: true
```
4. 配置风控阈值（至少确认）：  
```
trade:
  min_order_notional_usd: 5.0
  max_open_positions: 60
  max_global_exposure_frac: 0.6
  max_per_pair_exposure_frac: 0.25
  gas_emergency_pause_enabled: true
```
5. 启动服务，确认日志出现：  
   - `trade_engine starting enabled=true ...`  
   - `ENTRY_SIGNAL / ENTRY_READY`（alpha 日志中）  

**建议**：首次切到实盘前，先跑一段时间 `dry_run: true`，观察 alpha 日志与交易信号质量。

## 账户配置模板（Opinion）
建议将账户文件放在 `./config/accounts.json`，示例：
```json
{
  "accounts": [
    {
      "account_id": "opinion_acc",
      "exchange": "Opinion",
      "api_key": "OPINION_API_KEY",
      "secret_key": "OPINION_SECRET",
      "private_key": "0xYOUR_SIGNER_PRIVATE_KEY",
      "multi_sig_address": "0xYOUR_SAFE_OR_WALLET",
      "rpc_url": "https://bsc-dataseed.binance.org",
      "chain_id": 56,
      "host": "https://proxy.opinion.trade:8443"
    }
  ]
}
```

> 说明：该文件用于 Opinion 侧下单与签名。请确保私钥与 API Key 不被提交到仓库。
> 生成方式：
```bash
cp config/accounts.example.json config/accounts.json
```

## 最小化实盘配置示例
在 `config.yaml` 中至少配置这些字段（其它保持默认）：
```yaml
monitoring_db_url: postgresql://user:pass@127.0.0.1:5432/monitor
discovery_db_path: ./data/discovery.db
opinion_api_key: "YOUR_OPINION_API_KEY"

trade:
  enabled: true
  dry_run: false
  opinion_account_file: ./config/accounts.json
  opinion_account_id: opinion_acc
  opinion_enable_trading_on_startup: true
  min_order_notional_usd: 5.0
  max_open_positions: 20
  max_global_exposure_frac: 0.3
  max_per_pair_exposure_frac: 0.1
```

## 数据库
- **PostgreSQL**：核心监控数据（自动建表，schema 位于 `sql/schema_pg.sql`）
- **SQLite**：队列缓存（`db_queue_path`）

## 磁盘与日志容量建议（请务必注意）
- 建议使用 **500GB 以上磁盘**。  
- 在高频监测下，**日志一天可生成 100GB+**（取决于订阅市场数量与 alpha 记录频率）。  
- 建议把 `records/` 与数据库目录放在独立大盘，并定期归档/压缩（如按天 gzip）。  

## 日志归档/压缩（示例）
### 方案 A：简单脚本（按天切分 + gzip）
脚本已落地：`scripts/rotate_alpha_logs.sh`。  
可以配合 `cron` 每天执行一次：
```bash
0 2 * * * /opt/monitor/scripts/rotate_alpha_logs.sh >> /var/log/alpha_log_rotate.log 2>&1
```

### 方案 B：logrotate（自动轮转）
保存为 `/etc/logrotate.d/opinion-polymarket-monitor`：
```conf
/opt/monitor/records/alpha_log.jsonl /opt/monitor/records/alpha_log.csv {
  daily
  rotate 7
  compress
  delaycompress
  missingok
  notifempty
  copytruncate
  dateext
  dateformat -%Y%m%d
}
```

> `copytruncate` 适合不重启进程的场景；如果你能接受重启或改为 SIGHUP 通知，建议改为更严格的 rotate 策略。

## 常见问题
- **推不上交易所数据？** 检查 WS/REST URL 与 API Key。
- **没有配对数据？** 确认 matcher 已生成 `discovery.db`，且 `discovery_db_path` 配置正确。
- **不想交易？** 关闭 `trade.enabled` 或开启 `trade.dry_run`。

## 安全提示
- 不要提交包含密钥的 `config.yaml`。
- 生产环境请使用只读/最小权限的 API Key。
