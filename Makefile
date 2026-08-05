.PHONY: run build tidy healthz

# 正确入口是 ./cmd/worker，不要 go build my_transcode
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
