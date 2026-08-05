# 转码消息协议（草案 v1）

`my_service` 与 `my_transcode` 只通过 Kafka（或调试 HTTP）交换下列 JSON，**不共享业务库**。

## Topics

| Topic | 方向 | 说明 |
|-------|------|------|
| `media.transcode.jobs` | my_service → worker | 转码任务 |
| `media.transcode.results` | worker → my_service | 转码结果 |

Kafka message key 建议用 `job_id`，便于分区内有序与幂等。

## JobMessage（任务）

```json
{
  "schema_version": 1,
  "job_id": "vid_2_1710000000",
  "biz": "my",
  "biz_ref": "video:2",
  "input": {
    "bucket": "my-media",
    "key": "my/video/2026/08/05/xxxx.mp4"
  },
  "output": {
    "bucket": "my-media",
    "prefix": "my/hls/2/"
  },
  "profile": "h264_hls",
  "created_at": "2026-08-06T03:00:00+08:00"
}
```

| 字段 | 说明 |
|------|------|
| `job_id` | 全局唯一；重试原样重发，worker 幂等 |
| `biz` / `biz_ref` | 业务标识，worker 只透传回结果 |
| `input` | 原片对象 |
| `output.prefix` | HLS 目录前缀，最终播放 key 一般为 `{prefix}index.m3u8` |
| `profile` | 转码档位；第一期仅 `h264_hls` |

## ResultMessage（结果）

```json
{
  "schema_version": 1,
  "job_id": "vid_2_1710000000",
  "biz": "my",
  "biz_ref": "video:2",
  "status": "ready",
  "play_key": "my/hls/2/index.m3u8",
  "play_url": "http://127.0.0.1:19000/my-media/my/hls/2/index.m3u8",
  "duration_sec": 47,
  "error": "",
  "finished_at": "2026-08-06T03:05:00+08:00"
}
```

| `status` | 含义 |
|----------|------|
| `processing` | 可选进度（也可不发） |
| `ready` | 成功，可播 |
| `failed` | 失败，看 `error` |

## my_service 侧建议字段（后续再改）

在 `video` 表保留原片，另加：

- `play_url` / `play_key`
- `transcode_status`: `none|pending|processing|ready|failed`
- `transcode_error`
- `transcode_job_id`

触发：视频创建/更新且有 `source_key` 时发 Job；消费 Result 后回写。

## 幂等

同一 `job_id` 重复消费：若输出已存在且完整，直接回 `ready`；否则覆盖重转。
