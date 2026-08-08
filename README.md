# my_transcode

独立视频转码 Worker（Go + Gin 探活）。与 `my_service` 业务解耦：只认 MinIO 路径与 Kafka 消息，**不直连业务库**。

第一期目标：`H.264 + HLS`（`index.m3u8` + `.ts`）。

## 边界

| 做 | 不做 |
|----|------|
| 消费转码任务 | 用户 / 视频运营业务 |
| ffmpeg 出 H.264 HLS | 直连 PostgreSQL 改 `video` 表 |
| 上传 m3u8 + ts | 鉴权、上架、标题 |
| 产出结果消息 | 多业务域逻辑 |

```text
my_media（媒资中心）               Kafka                          my_transcode
──────────────────               ─────                          ────────────
预签名上传原片 → MinIO
发 JobMessage  ─────────────────► media.transcode.jobs ─────────► 下载→ffprobe→ffmpeg→上传 HLS
回写 status/play_url/duration ◄─ media.transcode.results ◄────── ResultMessage
```

已验收端到端链路：`上传 → Kafka → my_transcode → HLS → 详情 ready → 浏览器播 play_url`  
详见 sibling 仓 [`my_media/docs/m1-pipeline.md`](../my_media/docs/m1-pipeline.md)。

协议详见 [docs/protocol.md](docs/protocol.md)。

## 目录

```text
cmd/worker/           入口：Kafka 消费 + Gin /healthz
internal/
  protocol/           任务/结果 JSON 契约
  config/             配置
  kafka/              生产/消费（骨架，待接 SDK）
  minio/              对象存储（骨架，待接 SDK）
  ffmpeg/             转码命令封装
  job/                单任务编排
  httpapi/            /healthz 、可选 /debug/jobs
configs/              配置样例
docs/                 协议说明
scripts/              本机 ffmpeg 试跑
Dockerfile            含 ffmpeg 的运行镜像
```

## Docker 常驻（推荐，不用 IDE 起）

服务已并入 **`/Users/wangdante/D/mydocker/docker-compose.yml`**（网络 `jarven`，与 kafka/minio 同栈）。

```bash
# 方式 A：在 mydocker 目录
cd /Users/wangdante/D/mydocker
docker compose up -d --build my_transcode

# 方式 B：在本仓库 make（内部仍调 mydocker）
cd /Users/wangdante/D/kugou/my/my_transcode
make docker-up

curl http://127.0.0.1:8088/healthz
make docker-logs    # 日志
make docker-down    # 停止
```

容器内：`kafka:29092`（INTERNAL）、`minio:9000`；见 `configs/config.docker.yaml`。  
`publicURL` 仍是 `http://127.0.0.1:19000`。

**注意**：IDE 里若已起了 Worker，先停掉，避免抢 `8088` / consumer group。

## 本机直接跑（开发）

```bash
cp configs/config.example.yaml configs/config.yaml
go mod tidy
make run
curl http://127.0.0.1:8088/healthz
```

本机先验证 ffmpeg（不依赖服务）：

```bash
./scripts/transcode_local.sh /path/to/input.mp4 ./out_hls
```

## GoLand 运行

不要用 `go build my_transcode`（模块名不是可执行包路径）。

正确方式任选其一：

1. 打开 `cmd/worker/main.go`，点左上角绿色三角 **Run**
2. Run Configuration → Run kind: **Package** → Package path: `my_transcode/cmd/worker`，Program arguments: `-config configs/config.yaml`
3. 终端：`make run` 或 `go run ./cmd/worker -config configs/config.yaml`

## 分步落地

1. ~~协议 + 目录骨架~~
2. ~~本机 `scripts/transcode_local.sh` 跑通 HLS~~
3. ~~接通 MinIO / Kafka + `/healthz` 区分开关与连通~~（当前）
4. `POST /debug/jobs` 端到端联调（上传原片→转码→回写结果）
5. `my_service` 加 `play_url` / `transcode_status` 并联调

## 配置对齐 my_service 本地

- Kafka: `127.0.0.1:9092`
- MinIO: `127.0.0.1:19000` / bucket `my-media`
