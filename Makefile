.PHONY: run build tidy healthz docker-up docker-down docker-logs docker-rebuild

run:
	go run ./cmd/worker -config configs/config.yaml

run-example:
	go run ./cmd/worker -config configs/config.example.yaml

build:
	mkdir -p bin
	go build -o bin/worker ./cmd/worker

tidy:
	go mod tidy

healthz:
	curl -sS http://127.0.0.1:8088/healthz; echo

# ---- Docker：走 mydocker 主编排 ----
MYDOCKER := /Users/wangdante/D/mydocker

docker-up: ## 在 mydocker 中构建并启动 my_transcode
	cd $(MYDOCKER) && docker compose up -d --build my_transcode
	@echo "探活: curl http://127.0.0.1:8088/healthz"

docker-down:
	cd $(MYDOCKER) && docker compose stop my_transcode

docker-logs:
	docker logs -f my_transcode

docker-rebuild:
	cd $(MYDOCKER) && docker compose up -d --build --force-recreate my_transcode
