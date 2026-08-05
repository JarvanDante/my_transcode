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
		log.Printf("load %s failed: %v; fallback to config.example.yaml", *cfgPath, err)
		cfg, err = config.Load("configs/config.example.yaml")
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
	}

	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		log.Fatalf("workDir: %v", err)
	}

	store, err := minio.New(cfg.Minio)
	if err != nil {
		log.Fatalf("minio: %v", err)
	}
	if store.Enabled() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := store.EnsureBucket(ctx, cfg.Minio.Bucket); err != nil {
			log.Printf("warn: ensure bucket %s: %v", cfg.Minio.Bucket, err)
		} else {
			log.Printf("minio: bucket ready %s", cfg.Minio.Bucket)
		}
		cancel()
	}

	ff := ffmpeg.New(cfg.FFmpeg)
	bus := kafka.New(cfg.Kafka)
	defer bus.Close()
	if bus.Enabled() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		if err := bus.EnsureTopics(ctx); err != nil {
			log.Printf("warn: ensure kafka topics: %v", err)
		} else {
			log.Printf("kafka: topics ready")
		}
		cancel()
	}
	runner := job.New(cfg, store, ff, bus)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := bus.ConsumeJobs(ctx, runner.Handle); err != nil && ctx.Err() == nil {
			log.Printf("kafka consumer stopped: %v", err)
		}
	}()

	srv := httpapi.New(cfg, runner, store, bus)
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
