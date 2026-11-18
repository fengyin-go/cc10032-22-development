# 实时计算服务中心

面向实时数据分析场景，提供流式计算定义、窗口聚合、策略告警和状态管理的实时数据平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流式源端 | `stream_source` | 数据源配置 | 源端名称、消息队列、Topic名称 |
| 窗口配置 | `window_config` | 窗口策略 | 策略名称、窗口类型、窗口大小 |
| 计算算子 | `calc_operator` | 算子定义 | 算子名称、算子类型、输入字段 |
| 状态存储 | `state_store` | 状态配置 | 配置名称、后端类型、状态键模式 |
| 水位追踪 | `watermark_track` | 水位记录 | 水位名称、数据源、当前水位 |
| 告警策略 | `alert_policy` | 告警规则 | 规则名称、监控指标、触发条件 |
| 滑动指标 | `sliding_index` | 指标计算 | 指标名称、计算窗口、聚合函数 |
| 流关联 | `join_stream` | 关联配置 | 配置名称、左流源、右流源 |
| 结果落地 | `sink_output` | 输出配置 | 配置名称、输出类型、目标地址 |
| 补数任务 | `backfill_job` | 补数作业 | 作业名称、目标指标、补数范围 |
| CEP规则 | `cep_rule` | 事件序列 | 序列名称、事件类型、匹配顺序 |
| 吞吐管理 | `throughput_mgr` | 吞吐配置 | 配置名称、数据源、最大QPS |
| 检查点 | `checkpoint_mgr` | 检查点配置 | 配置名称、间隔时间、超时时长 |
| 故障恢复 | `restore_plan` | 恢复方案 | 方案名称、任务范围、恢复模式 |
| 指标血缘 | `metric_lineage` | 血缘记录 | 记录ID、指标名称、上游源端 |
| 迟到处理 | `late_handler` | 处理策略 | 策略名称、允许延迟、迟到动作 |
| 任务拓扑 | `task_topology` | 拓扑配置 | 拓扑名称、算子链、并行度 |
| 数据采样 | `data_sampler` | 采样策略 | 策略名称、采样方式、采样比例 |
| 调试追踪 | `debug_trace` | 调试会话 | 会话名称、目标算子、采样数据 |
| 聚合落库 | `aggregate_sink` | 聚合任务 | 任务名称、源指标集、聚合粒度 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 指标血缘版本与回放验证

指标血缘不再只保存一段路径文本，而是维护一份**路径版本台账**
（`app/store.py::_lineage_ledger`），每条血缘记录都绑定一个「来源批次 + 路径版本」。

- 刷新：`POST /api/metric-lineage/refresh`（也可走行内动作「刷新血缘」）。
  可由检查点驱动，记录检查点快照时点；同一指标、同一路径、同一检查点的并发
  或重复刷新复用同一个版本，不产生两个当前版本。
- 版本台账：`GET /api/metric-lineage/versions?metric=...`；
  单版本详情（含路径核对、检查点、回放报告）：
  `GET /api/metric-lineage/versions/{version_id}`。
- 确认：`POST /api/metric-lineage/confirm`。只有仍为当前版本、且实时算子链
  未变化的版本才能确认；历史版本不能被确认或标记断链。
- 健康同步：读取列表/详情前会对照 `calc_operator`、`sink_output` 等实时模块，
  算子缺失记为「断链」、路径变化记为「变更中」，算子链调整后旧血缘不再显示正常。

回放引擎（`app/services/replay_engine.py`）读取血缘版本与检查点：

- `GET /api/replay-engine/validate` 核对当前版本、可用检查点与历史验证报告；
- `POST /api/replay-engine/validate` 创建回放任务（可带 `execute` 直接执行），
  只接受已确认且算子链完整的血缘版本，并在锁内固定创建时点的版本
  （`_lineage_pinned_at`）；
- `POST /api/replay-engine/{task_id}/execute` 执行回放并生成回放验证报告，
  报告写清算子缺失、路径变化、使用的检查点与检查点快照时点；
- `GET /api/replay-engine/history`、`GET /api/replay-engine/verification`
  供血缘详情页按路径版本核对验证结果。

并发与一致性：血缘刷新与回放创建共用 `store.lock()`，先拿到锁的一方决定快照
时点；写入走 `store.transaction(...)`，失败时回滚到上一份完整版本。

前端在「指标血缘」页面内嵌 `frontend/src/views/LineagePanel.vue`：
上方是路径版本台账与 DAG 路径，下方是回放历史与验证报告，复核人员可直接
确认版本、以某个版本创建回放并核对结果。

领域：数据工程
子方向：数据分析与数据应用
