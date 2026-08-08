# 构建
FROM golang:1.22-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/worker ./cmd/worker

# 运行：带 ffmpeg + curl(健康检查)
FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /out/worker /app/worker
# 默认配置；compose 会挂载 config.docker.yaml 覆盖
COPY configs/config.docker.yaml /app/configs/config.yaml
ENV CONFIG=/app/configs/config.yaml
EXPOSE 8088
CMD ["/app/worker", "-config", "/app/configs/config.yaml"]
