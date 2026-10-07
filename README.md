# 星载回传分区交接调度服务

地面站切换接收实例时，保证**任一代次内一个分区至多归属一台实例**的调度服务。
核心是「撤销 — 确认 — 原子发布」交接协议，SQLite 单文件持久化，仅依赖 Python 3.11 标准库。

## 不变量

1. 撤销中的分区仍记在旧实例名下；新实例在旧实例确认前拿不到该分区，
   因而旧实例迟到的确认与新实例不会并行消费。
2. 旧实例确认后，以下动作在**同一个持久化事务**中完成：
   删除旧所有权 → 转授目标实例 → 推进可交接集合 → 公布新代次。
3. 任何读取只能看到两种状态：旧完整分配，或与已确认释放一致的中间分配。
   进程在释放后、发布前崩溃并重启，事务整体回滚，不会双重归属。
4. 确认过期（无进行中交接）、越权（非当前持有者）、含多余分区一律拒绝，且不推进代次。
5. 稳定请求标识幂等：相同请求重传返回首次结果；同一标识携带不同快照明确冲突（409）。
6. 交接进行中收到新快照会被暂时拒绝（409），响应内带**已持久化目标**，
   调用方必须据此重新收敛，而不是按本地旧状态抢占。
7. 每次快照的结算回执按快照请求标识持久化：直接收敛立即给出发布代次与
   全部目标成员；进入撤销的交接在最后一次有效确认后定格为唯一完成回执，
   固定开始时持久化的目标与每个释放方实际确认的分区。确认重放、过期或
   被拒确认不推进回执；后续代次的交接不得改写历史回执。

目标分配完全由「成员标识 + 分区号」确定：`target = sorted(members)[int(part) % len(members)]`，
因此重算结果稳定且与顺序无关。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康端点，200 `{"status":"ok"}` |
| GET | `/v1/assignments[?member=]` | 当前分配读模型；`revoking` 字段标注正在撤销的目标 |
| GET | `/v1/handover` | 当前交接：持久化目标、待撤销、已释放集合 |
| GET | `/v1/receipts/{request_id}` | 该次快照的结算回执：`stable`/`revoking`/`completed` |
| POST | `/v1/snapshots` | `{"request_id","members"}` 提交完整成员快照 |
| POST | `/v1/confirms` | `{"request_id","member","parts"}` 旧实例确认撤销 |

状态码：`200` 稳定/已记录，`202` 已进入撤销，`400` 参数或多余分区，
`403` 越权确认，`404` 回执标识不存在，`409` 确认过期 / 幂等冲突 / 交接进行中，`503` 存储不可用。

### 结算回执

值班员按成员快照的稳定请求标识查询该次交接的结算回执，无需从当前分配
或逐分区链路反推：

* `stable`：直接收敛，回执立即给出发布代次 `epoch` 与全部目标成员 `members`；
* `revoking`：进行中（不冒充完成），`pending` 列出仍待释放的旧成员分区，
  `released` 按释放方归集已实际确认的分区；
* `completed`：最后一次有效确认后的唯一完成回执，固定本次开始时持久化的
  `target`、每个释放方实际确认的分区与完成代次；即使随后发起新的成员替换，
  查询旧标识也不会混入后续代次。

回执随交接协议同事务持久化，服务重启后可通过 HTTP 查询到同一结果。

### 典型时序

```http
POST /v1/snapshots {"request_id":"r1","members":["a","b"]}   -> 200 stable
POST /v1/snapshots {"request_id":"r2","members":["b","c"]}   -> 202 revoking
GET  /v1/assignments?member=c                                -> 0 个分区（确认前拿不到）
POST /v1/confirms  {"request_id":"c1","member":"a","parts":["0","2","4"]}  -> 200 partially_released, epoch 1
GET  /v1/receipts/r2   -> 200 revoking, released={"a":["0","2","4"]}, pending=[1,3,5]
POST /v1/confirms  {"request_id":"c2","member":"b","parts":["1","3","5"]}  -> 200 completed, epoch 2
GET  /v1/receipts/r2   -> 200 completed, 固定 target 与各释放方确认分区
# 任一请求重传（同 request_id 同体）-> 原样返回首次结果
```

## 运行

### Docker Compose

```bash
docker compose up -d --build
curl http://127.0.0.1:8080/healthz

# 可配置宿主绑定与端口
HOST_BIND=0.0.0.0 HOST_PORT=9090 docker compose up -d
# 分区数
PARTITION_COUNT=1024 docker compose up -d
```

### 单次 verify 容器

围绕**成员替换、失效确认、中断恢复**运行规则测试、镜像构建自检与 API 冒烟，
以状态码退出（0 成功）：

```bash
docker compose --profile verify run --rm verify
echo $?
```

### 本地直接运行（无需第三方依赖）

```bash
python verify.py                      # 与 verify 容器执行内容相同
PORT=8080 DB_PATH=./data/handoff.db python -m app.server
python -m unittest discover -s tests
```

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | 服务监听地址 |
| `PORT` | `8080` | 服务监听端口 |
| `DB_PATH` | `/data/handoff.db` | SQLite 持久化文件 |
| `PARTITION_COUNT` | `256` | 分区总数（首次初始化后固定） |
| `HOST_PORT` / `HOST_BIND` | `8080` / `127.0.0.1` | Compose 宿主端口与绑定地址 |

## 目录

```
app/store.py     持久化与交接协议（单事务原子发布、幂等、恢复、结算回执）
app/server.py    标准库 HTTP 服务
verify.py        单次校验入口（规则测试 + 构建自检 + API 冒烟）
tests/           34 个规则/HTTP 测试，含崩溃注入与跨连接并发
```
