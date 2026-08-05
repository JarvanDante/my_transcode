package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"my_transcode/internal/config"
	"my_transcode/internal/ffmpeg"
	"my_transcode/internal/httpapi"
	"my_transcode/internal/job"
	"my_transcode/internal/kafka"
	"my_transcode/internal/minio"
)

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "config file path")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		// 允许直接用 example 启动骨架
		log.Printf("load %s failed: %v; fallback to config.example.yaml", *cfgPath, err)
		cfg, err = config.Load("configs/config.example.yaml")
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
	}

	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		log.Fatalf("workDir: %v", err)
	}

	store := minio.New(cfg.Minio)
	ff := ffmpeg.New(cfg.FFmpeg)
	bus := kafka.New(cfg.Kafka)
	runner := job.New(cfg, store, ff, bus)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := bus.ConsumeJobs(ctx, runner.Handle); err != nil && ctx.Err() == nil {
			log.Printf("kafka consumer stopped: %v", err)
		}
	}()

	srv := httpapi.New(cfg, runner)
	httpSrv := &http.Server{Addr: cfg.HTTP.Addr, Handler: srv.Engine()}
	go func() {
		log.Printf("http listen %s (healthz /healthz)", cfg.HTTP.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	_ = httpSrv.Shutdown(shutdownCtx)
}
