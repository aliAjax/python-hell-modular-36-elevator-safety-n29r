# 电梯与自动扶梯巡检和事件响应

这是一个只使用Python标准库和SQLite的模块化原型项目，默认端口为`8336`。领域对象包括设备、检验、维保、困人报警、救援任务、整改证据和恢复许可。`app.py`只负责参数解析、依赖组装和服务生命周期，业务状态机与约束集中在`src/rules.py`。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、领域异常、身份解析和实体数据结构。
- `src/rules.py`：状态机、角色权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务、乐观锁、审计和幂等键。
- `src/service.py`：用例编排、离线记录合并、版本控制和审计写入。
- `src/ledger.py`：补录批次、事件账本、按时点回放、结论失效重算和许可复核。
- `src/http_api.py`：HTTP路由、JSON解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8336
```

服务启动时自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。初始化服务不需要单独命令，首次启动即可访问：

```bash
curl http://127.0.0.1:8336/health
```

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。
- `POST /api/backfill-batches`：提交补录批次`{"batch_key":"可选","records":[...]}`。
- `GET /api/backfill-batches` / `GET /api/backfill-batches/<id>`：查询批次与回放报告。
- `POST /api/backfill-batches/<id>/replay`：回放失败后按原批次重试。
- `GET /api/ledger/events?equipment_id=&kind=`：查询账本事件。
- `GET /api/ledger/replay?equipment_id=<id>&as_of=<时间>`：按时点回放设备状态。
- `GET /api/ledger/conclusions?equipment_id=`：查询结论及其失效历史。
- `GET /api/ledger/reviews`：待复核许可列表（含设备）。

身份通过`X-User-Id`和`X-Role`请求头传入，角色和动作权限由规则引擎校验。## 核心流程

创建设备后安排检验、维保和困人报警；报警派发救援任务，完成后才能解决。整改证据通过复核后关闭，恢复运行许可必须基于有效的检验和已关闭整改。

## 规则重点

- 同一设备编号不能重复创建；同一设备和故障代码不能同时存在多个未关闭报警。
- 组件更换维保必须填写`part_serial`。
- 恢复许可受设备状态、通过检验和未关闭整改共同限制。

## 补录与时间账本

设备检验、报警、整改和恢复许可都会作为带业务时间（`event_time`）的事件进入账本；在线操作自动镜像入账，现场断网时巡检员在本地记录，回网后按批次补录。补录记录格式：

```json
{"batch_key": "可选批次键", "records": [{"stable_id": "设备内稳定编号", "kind": "inspection|alarm|remediation|permit", "event_type": "见下表", "equipment_id": "...", "ref_id": "关联对象id", "event_time": "ISO-8601", "payload": {}}]}
```

事件类型：`inspection/result_recorded`（`payload.result`为`passed`或`failed`）、`alarm/raised|closed`、`remediation/opened|closed`、`permit/granted|revoked`。提交和重试需要`inspector`或`admin`角色。

- **去重与并发**：`stable_id`全局唯一，重复补录不改写先入账的记录，内容不一致时报告`content_mismatch`；`batch_key`提供批次级幂等。两名巡检员同时提交同一批记录时写事务串行化，先入账的生效，后到批次全部记为重复。
- **结论失效重算**：补录事件的业务时间早于某结论的时点时，按完整历史重算该结论；结果变化则旧结论置为`invalidated`并写入新结论，依据事件序号（`basis`）随结论保存，巡检结论因此能说清当时依据。只影响当前状态（晚于所有结论时点）的补录不动历史结论，已发许可照旧生效。
- **许可复核**：重算后不再成立的许可转为`pending_review`，许可实体同步改状态并写审计，涉及设备列入`permits_pending_review`和`GET /api/ledger/reviews`。
- **失败重试**：回放为整批事务，任一步失败则派生写入全部回滚，批次与事件保留并标记`replay_failed`，修正数据后用`POST /api/backfill-batches/<id>/replay`原批重试。
- **按时点回放**：`GET /api/ledger/replay?equipment_id=<id>&as_of=<时间>`按`(event_time, seq)`顺序重放到指定时点，返回当时的检验结论、未关闭报警与整改、许可状态及所依据的事件清单。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

项目使用请求头模拟身份、SQLite单机持久化和简化状态机，适合原型演示和流程验证，不替代行业正式系统、设备控制系统或现场安全规程。
