# 科研影像 DICOM 统一标识入库平台

合并多次 DICOM 导出时，为同一研究项目维护**持久、跨批次稳定**的标识映射（PatientID、
Study/Series/SOP Instance UID），对入库文件做统一重编码并以整批 ZIP 下载。
研究结束后可按项目 ID 发起**销毁**：永久清理项目影像与源标识映射，仅保留项目 ID 与销毁状态。
基于 **FastAPI + pydicom + SQLite**，提供可浏览器操作的入库网页与 Docker Compose 一键环境。

---

## 1. 能力与约束（对照需求）

| 需求 | 实现 |
| --- | --- |
| 选择研究项目 | 内置两个种子项目，网页可新建；映射按 `(project_id, id_kind, source_value)` 维度**项目间完全隔离** |
| 上传最多 20 份、合计 ≤16 MiB | 路由层流式累计大小 + 流水线二次校验，超限整批拒绝（HTTP 413），可用环境变量调整 |
| 仅未压缩 DICOM | 只接受 Implicit/Explicit VR Little Endian（`1.2.840.10008.1.2[.1]`），Deflated/JPEG/RLE 等一律拒绝 |
| 整批 ZIP 下载 | 重编码后打包为 `<batch_id>.zip`，原子 `rename` 发布后才可下载 |
| 四类标识持久映射 | PatientID 与 Study/Series/SOP Instance UID；同项目同源标识跨批次沿用同一目标 |
| 嵌套序列引用替换 | 递归进入**任意** VR=SQ（Referenced SOP/Study/Series/Patient、OriginalAttributes 等），不限嵌套深度 |
| 目标尚未入库先分配 | 保留阶段对嵌套中出现的前向引用（如未来 SOP）同样预分配映射并随批发布 |
| SOP Class UID 与像素不变 | 白名单保护 `SOPClassUID`/`MediaStorageSOPClassUID`/`ReferencedSOPClassUID`；像素字节改写前后逐字节比对，传输语法不变 |
| 转换期不持数据库写锁 | 写锁仅存在于毫秒级的“映射保留”和“发布”两个短事务，转换为纯文件/CPU 操作，无事务 |
| 并发批次同源不产生不同结果 | `BEGIN IMMEDIATE` + 唯一约束 + 不持锁轮询等待，他批次 PENDING 时等待其发布/回滚，保证同一源标识全局唯一目标 |
| 整批文件与新增映射一起发布 | 发布单事务：批次置 PUBLISHED + 本批 PENDING 映射置 PUBLISHED；zip 先原子落盘 |
| 坏文件/失败/中断不留部分批次 | 解析失败零写入；转换/发布失败回收本批 PENDING 映射并删暂存；启动恢复回滚所有中断 PENDING，已发布结果永不改动 |
| 重启保留已发布结果 | SQLite 与 published ZIP 位于持久卷 `./data`（compose 命名卷），重启后映射与下载均保留 |
| **按项目 ID 发起销毁、查看进度** | `POST /api/projects/{id}/destroy` 受理（202），`GET /api/projects/{id}/destroy` 查进度；网页「项目销毁」卡片一键操作并实时轮询 |
| **受理后拒绝新上传/下载，批次与映射不可见** | 受理即写墓碑；上传/下载路由在进程级许可锁内检查墓碑，批次/映射/详情接口立即 410，全局列表同步过滤 |
| **在途转换不得再发布，已获准下载可完成** | 发布事务内联查销毁墓碑（原子闸门），在途批次到发布时被拦截回滚；已放行下载计入在途，流式发送完整结束后才释放 |
| **等转换退出、下载关闭后物理删除** | 销毁线程等在途计数归零（条件变量，不持 SQLite 锁）→ 删 ZIP/暂存（不持锁）→ 短事务删批次/映射/项目行并标记 DESTROYED，仅留墓碑 |
| **重复请求跟随同一次销毁，断连仍继续** | `INSERT OR IGNORE` 幂等墓碑，重复 POST 返回同一记录；销毁由独立后台线程推进，与 HTTP 请求生命周期无关 |
| **删除失败保持销毁中并可重试** | 失败记录 `last_error`/`attempts` 并保持 DESTROYING，销毁线程按间隔自动重试，重复 POST 也会重新拉起线程 |
| **进程退出后重启自动续完** | 启动时扫描 DESTROYING 墓碑并重启销毁线程；删除流程幂等重放，绝不提前报完成、不留迟到产物 |
| **等待和文件清理不持 SQLite 写锁** | 等待为纯内存条件变量，文件删除在事务外；仅“登记墓碑/删行标记/失败记录”为毫秒级短事务，其他项目照常使用 |
| **重启不得重建已销毁项目** | 种子项目按名称查销毁墓碑，已销毁者永久跳过；已销毁项目行已删除，不会出现在项目列表 |

---

## 2. 目录结构

```
.
├── app/
│   ├── config.py        # 环境变量配置（限额、数据目录、锁等待、销毁重试、演示钩子）
│   ├── ids.py           # 四类标识、标签规则、UID 生成
│   ├── db.py            # SQLite 表结构、保留/发布/回滚/恢复协议、销毁墓碑与闸门
│   ├── dicom.py         # pydicom 解析、校验、递归重写、像素/保护标签断言
│   ├── pipeline.py      # 批次流水线（校验→保留→转换→原子发布）与启动恢复
│   ├── destroy.py       # 项目销毁：受理、在途等待、物理删除、失败重试、重启续办
│   ├── main.py          # FastAPI 路由（项目/批次/映射/下载/销毁/静态页）
│   └── web/index.html   # 浏览器入库与销毁界面（原生 JS，无构建依赖）
├── scripts/
│   ├── generate_samples.py  # 生成含嵌套引用、前向引用、压缩/坏文件的合成样例
│   ├── demo.py              # 端到端验收（真实 HTTP + 多线程同源并发 + 项目销毁）
│   └── verify.sh            # 容器内一键验收
├── tests/               # pytest：25 个单元/集成测试（含 8 个销毁场景）
├── Dockerfile
├── docker/entrypoint.sh
├── docker-compose.yml
└── requirements.txt
```

---

## 3. 快速启动（Docker Compose，推荐）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000
```

- 首次启动自动建库、写入两个种子项目、生成合成样例到卷内 `/data/samples`。
- 数据持久化在命名卷 `dicom-data`（容器内 `/data`）：
  - `/data/registry.db`（SQLite，WAL 模式）
  - `/data/staging/`（批次暂存，成功/失败后均清空）
  - `/data/published/<batch_id>.zip`（已发布整批 ZIP）
- 健康检查：`GET http://localhost:8000/api/health`。

容器内一键验收（另开终端）：

```bash
docker compose exec dicom-remap scripts/verify.sh
```

停止与清理：

```bash
docker compose down            # 保留数据卷
docker compose down -v         # 同时删除数据卷（清空所有批次与映射）
```

### 限额环境变量（docker-compose.yml）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DICOM_REMAP_MAX_FILES` | 20 | 单批文件数上限 |
| `DICOM_REMAP_MAX_TOTAL_BYTES` | 16777216 | 单批合计字节上限（16 MiB） |
| `DICOM_REMAP_LOCK_WAIT` | 60 | 并发冲突时等待他批次发布/回滚的最长秒数 |
| `DICOM_REMAP_DESTROY_RETRY_SECONDS` | 2 | 销毁删除失败后的自动重试间隔（秒） |
| `DICOM_REMAP_SLOW_CONVERT_SECONDS` | 0 | 演示钩子：每份文件转换前额外休眠秒数 |
| `DICOM_REMAP_SLOW_DOWNLOAD_CHUNK_SECONDS` | 0 | 演示钩子：下载每个 64 KiB 分块前额外休眠秒数 |
| `GENERATE_SAMPLES` | true | 首次启动是否生成合成样例 |

> compose 默认开启慢转换（2s/文件）与慢下载（1s/分块）两个演示钩子，便于在浏览器
> 演示“慢转换/慢下载期间销毁”。生产部署时将它们置 `0` 即可关闭。

---

## 4. 本地非 Docker 运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

export DICOM_REMAP_DATA=./data
python scripts/generate_samples.py ./data/samples     # 可选：生成样例
uvicorn app.main:app --host 0.0.0.0 --port 8000
# 浏览器打开 http://localhost:8000
```

运行测试与端到端演示：

```bash
pytest tests/ -q
python scripts/demo.py --samples ./data/samples
```

---

## 5. 浏览器操作流程

1. 顶部下拉选择研究项目（或输入名称新建）。
2. 点击/拖拽选择最多 20 份未压缩 DICOM，界面实时显示份数与合计大小。
3. 点击「上传并入库整批」：成功后批次出现在列表，状态 `PUBLISHED`；失败（坏文件、压缩、超限）
   整批拒绝并提示原因，**不会产生任何部分批次或映射污染**。
4. 在「已发布映射」页签查看四类源→目标标识；点击批次行查看详情并**下载重编码整批 ZIP**。
5. 研究结束后，在「④ 项目销毁」卡片点击「永久销毁当前选中项目」并确认：
   受理后该项目立即变为只读墓碑（上传/下载关闭、批次与映射隐藏），
   页面每秒轮询展示销毁进度（在途转换/下载数量、重试次数、最近错误），
   完成后项目从列表消失，仅「销毁记录」中保留项目 ID 与 `DESTROYED` 状态。

### 演示：慢转换 / 慢下载期间销毁 + 重启续办

compose 默认开启演示钩子（`DICOM_REMAP_SLOW_CONVERT_SECONDS=2`、
`DICOM_REMAP_SLOW_DOWNLOAD_CHUNK_SECONDS=1`），可完整演示销毁的并发语义：

1. **慢转换期间销毁**：上传一个批次（转换约需 2s/文件），转换进行中立即在
   「④ 项目销毁」发起销毁——该批次到发布时被拦截回滚（不得发布），
   销毁线程等它退出后删净数据。
2. **慢下载期间销毁**：点击「下载 ZIP」后（分块限速，下载持续数秒）立即销毁——
   已获准的下载完整完成，销毁进度显示「等待 1 个下载」，下载关闭后才删除 ZIP。
3. **重启续办**：销毁处于 DESTROYING 时执行 `docker compose restart`，
   服务重启后自动续完销毁（墓碑驱动，幂等重放），最终进入 DESTROYED，
   已销毁项目不会被种子数据重建。

---

## 6. HTTP API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/health` | 健康检查 |
| GET/POST | `/api/projects` | 项目列表（含销毁状态）/ 新建（表单 `name`） |
| POST | `/api/projects/{id}/destroy` | 受理项目销毁（幂等，202）；重复请求跟随同一次销毁 |
| GET | `/api/projects/{id}/destroy` | 销毁进度（状态、时间戳、在途计数、重试/错误） |
| GET | `/api/destructions` | 全部销毁记录（墓碑列表） |
| POST | `/api/batches` | 上传批次（multipart：`project_id` + 多个 `files`），返回批次记录 |
| GET | `/api/batches?project_id=` | 批次列表（可按项目过滤；销毁中项目 410） |
| GET | `/api/batches/{id}` | 批次详情含文件清单（销毁中项目 410） |
| GET | `/api/batches/{id}/download` | 下载已发布整批 ZIP（销毁受理后 410） |
| GET | `/api/mappings?project_id=` | 已发布映射（可按项目过滤；销毁中项目 410） |
| GET | `/api/meta` | 当前限额与标识类型 |

示例：

```bash
curl -X POST http://localhost:8000/api/batches \
  -F project_id=<项目ID> \
  -F files=@a1.dcm -F files=@a2.dcm

# 发起销毁并轮询进度
curl -X POST http://localhost:8000/api/projects/<项目ID>/destroy
curl http://localhost:8000/api/projects/<项目ID>/destroy
```

---

## 7. 并发一致性与原子性设计

### 7.1 批次状态机

```
PENDING ──发布事务成功──▶ PUBLISHED（终态，永不回滚）
   │
   └──解析失败 / 转换异常 / 发布异常 / 进程中断 / 项目销毁──▶ FAILED（PENDING 映射同步回收）
```

### 7.2 映射保留协议（保证同源同果）

1. **保留（短写锁）**：单条 `BEGIN IMMEDIATE` 事务内登记 PENDING 批次，并对每个源标识
   `INSERT OR IGNORE` 一条映射。唯一约束 `UNIQUE(project_id, id_kind, source_value)` 兜底；
   若标识已被他批次 PENDING 持有，则本批次**释放锁后轮询等待**，直到对方发布（复用其目标）
   或回滚（删除 PENDING 行后自行保留）。
2. **转换（不持锁）**：仅做 pydicom 重写、像素/保护标签断言、ZIP 打包，无数据库事务。
3. **发布（短写锁）**：ZIP 原子 `os.replace` 到 published 后，单事务把批次与本批新建映射
   一并置 PUBLISHED。

由此，并发批次不可能给同一源标识分配不同目标；等待超时则明确返回 409，绝不静默分叉。

### 7.3 失败与中断

- 解析/限额阶段失败：数据库零写入，仅清理暂存目录。
- 转换/发布阶段失败：`fail_batch` 删除本批全部 PENDING 映射、批次置 FAILED、清理暂存与半成品 ZIP。
- 进程中断：下次启动 `recover_on_startup` 回滚所有 PENDING 批次与映射、清理暂存孤儿和
  无主 ZIP；已 PUBLISHED 的数据与文件一律保留。

### 7.4 项目销毁状态机与并发闸门

```
（无记录）──POST destroy──▶ DESTROYING ──删除流程全部成功──▶ DESTROYED（终态墓碑）
                              ▲  │删除失败：记录 last_error/attempts，保持 DESTROYING
                              └──┴── 按 DICOM_REMAP_DESTROY_RETRY_SECONDS 自动重试；
                                     进程重启由 resume_on_startup 重新拉起销毁线程
```

- **受理（幂等）**：`INSERT OR IGNORE` 写入墓碑（project_id 主键），重复请求、断连重试
  都跟随同一次销毁；受理与“放行新上传/下载”共用进程级许可锁，杜绝受理瞬间漏计在途操作。
- **三重闸门（受理即生效）**：
  1. 路由层：上传/下载在许可锁内查墓碑，拒绝新请求（410/409）；
  2. 保留事务：`reserve_mappings` 联查墓碑，拒绝新的批次/映射行；
  3. 发布事务：`publish_batch` 联查墓碑，在途批次到发布时被拦截回滚——
     受理前开始的转换**不得再发布**，其暂存与半成品 ZIP 随回滚清理。
- **在途等待（不持 SQLite 锁）**：销毁线程用条件变量等待该项目在途转换/下载计数归零。
  已获准的下载完整流式发送（自定义 Response 在 ASGI 调用结束时确定性地归还计数，
  客户端断连亦不泄漏）；计数归零前不触碰任何项目文件。
- **物理删除（先文件后行）**：先删 `published/<batch>.zip` 与 `staging/<batch>/`
  （不持锁），再用一条短事务删除 batch_files/id_mappings/batches/projects 行并把墓碑
  置为 DESTROYED。崩溃于任一步：重启后按墓碑幂等重放——文件删除可重复（missing_ok），
  行删除与状态标记同事务——**绝不提前报完成，也不留迟到产物**。
- **隔离性**：所有闸门、计数与删除都按 project_id 维度，其他项目的上传/下载/查询不受影响；
  销毁中的等待与文件清理不持 SQLite 写锁，全库读写照常。
- **不重建**：种子项目按名称查墓碑，已销毁者永久跳过；DESTROYED 项目的研究数据
  （ZIP/暂存/批次/映射/项目行）已全部物理删除，仅墓碑保留项目 ID、名称与销毁状态。

---

## 8. 样例与验收场景

`scripts/generate_samples.py` 生成固定 UID 的合成数据：

- `batch_a/`：两个实例，实例 2 嵌套引用实例 1 的 SOP、本研究 Study、患者，
  并含一个**前向引用**（目标 SOP 尚未入库）。
- `batch_b/`：新实例跨批次引用 `batch_a` 的 Series 与 SOP。
- `compressed/`：Deflated 压缩 DICOM，验证压缩拒绝。
- `bad/`：损坏文件，验证坏文件整批拒绝。

`scripts/demo.py` 覆盖七类断言（共 40+ 检查）：嵌套/前向引用替换、SOP Class 与像素不变、
跨批次映射复用、两线程同源并发结果唯一、项目隔离、坏文件/压缩/混合批次拒绝、20 份限额，
以及**项目销毁全流程**（幂等受理、上传/下载/列表闸门、物理删除、墓碑保留、他项目不受影响）。
`tests/` 另含 25 个 pytest 用例，含 4 线程同源并发、中断恢复、慢转换/慢下载期间销毁、
下载断连计数释放、删除失败重试与重启续办等场景。

