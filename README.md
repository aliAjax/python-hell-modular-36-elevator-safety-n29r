# 电梯与自动扶梯巡检和事件响应

这是一个只使用Python标准库和SQLite的模块化原型项目，默认端口为`8336`。领域对象包括设备、检验、维保、困人报警、救援任务、整改证据和恢复许可。`app.py`只负责参数解析、依赖组装和服务生命周期，业务状态机与约束集中在`src/rules.py`。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、领域异常、身份解析和实体数据结构。
- `src/rules.py`：状态机、角色权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务、乐观锁、审计和幂等键。
- `src/service.py`：用例编排、离线记录合并、版本控制和审计写入。
- `src/backfill.py`：离线补录批次的时点回放、稳定编号去重、结论重算与许可复核。
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
- `POST /api/backfill`：提交离线补录批次，请求体为`{"batch_id":"可选","source_id":"可选","records":[...]}`。
- `POST /api/backfill/<batch_id>/retry`：用批次原记录重放失败批次。
- `GET /api/backfill`：列出批次；`GET /api/backfill/<batch_id>`：读取批次报告。

身份通过`X-User-Id`和`X-Role`请求头传入，角色和动作权限由规则引擎校验。

## 离线补录的时点回放

现场断网时先把检验、报警、整改和恢复许可记在本地，回网后按批次补录。补录记录带`event_time`（事件发生时点）和稳定身份`(source_id, record_id)`，服务端把它们接成按时点回放的账：

- **按时点回放**：批次内记录按`event_time`排序入账，实体保留事件时点而非入账时点。
- **稳定编号去重**：同一`(source_id, record_id)`只入账一次；重复提交整批时先入账的生效，后到的判为重复，不重复落实体。
- **结论失效重算**：每批回放后重算受影响设备的适任结论（依据=设备状态+最近一次通过检验+未关闭整改），结论带当时依据；结论较上一版发生变化的设备记入`invalidated_conclusions`。
- **站不住的许可转待复核**：若晚到的补录改变了许可授予时点的依据（例如授予时点前出现了未通过检验或未关闭整改），该许可转为`pending_review`，并在`review_required`中列出设备与许可；只改当前状态的补录不影响过去已生效的许可。
- **整批失败保留原批次重试**：整批回放失败时不写入任何实体，批次（含原始记录）保留为`failed`，可通过`retry`接口用原记录重试。

批次报告字段：`status`（`committed`/`failed`）、`applied`、`deduped`、`conclusions`、`invalidated_conclusions`、`review_required`、`error`。## 核心流程

创建设备后安排检验、维保和困人报警；报警派发救援任务，完成后才能解决。整改证据通过复核后关闭，恢复运行许可必须基于有效的检验和已关闭整改。

## 规则重点

- 同一设备编号不能重复创建；同一设备和故障代码不能同时存在多个未关闭报警。
- 组件更换维保必须填写`part_serial`。
- 恢复许可受设备状态、通过检验和未关闭整改共同限制。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

项目使用请求头模拟身份、SQLite单机持久化和简化状态机，适合原型演示和流程验证，不替代行业正式系统、设备控制系统或现场安全规程。
