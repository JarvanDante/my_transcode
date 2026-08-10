package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"my_transcode/internal/config"
	"my_transcode/internal/job"
	"my_transcode/internal/kafka"
	"my_transcode/internal/minio"
	"my_transcode/internal/protocol"
)

type DepStatus struct {
	Enabled   bool   `json:"enabled"`
	Connected bool   `json:"connected"`
	Consuming bool   `json:"consuming,omitempty"` // 仅 kafka 消费者用：消费循环是否存活
	Error     string `json:"error,omitempty"`
}

type Server struct {
	cfg    *config.Config
	runner *job.Runner
	store  *minio.Client
	bus    *kafka.Bus
	engine *gin.Engine
}

func New(cfg *config.Config, runner *job.Runner, store *minio.Client, bus *kafka.Bus) *Server {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), gin.Logger())

	s := &Server{cfg: cfg, runner: runner, store: store, bus: bus, engine: r}
	r.GET("/healthz", s.healthz)
	if cfg.Debug.AllowSubmit {
		r.POST("/debug/jobs", s.debugJob)
	}
	return s
}

func (s *Server) Engine() *gin.Engine { return s.engine }

func (s *Server) healthz(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 4*time.Second)
	defer cancel()

	minioSt := s.checkMinio(ctx)
	kafkaSt := s.checkKafka(ctx)

	// 进程存活；ready=所有已开启依赖都连通
	ready := true
	if minioSt.Enabled && !minioSt.Connected {
		ready = false
	}
	if kafkaSt.Enabled && (!kafkaSt.Connected || !kafkaSt.Consuming) {
		ready = false
	}

	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}

	c.JSON(status, gin.H{
		"ok":      true, // 进程存活
		"ready":   ready,
		"service": "my_transcode",
		"time":    time.Now().Format(time.RFC3339),
		"minio":   minioSt,
		"kafka":   kafkaSt,
	})
}

func (s *Server) checkMinio(ctx context.Context) DepStatus {
	st := DepStatus{Enabled: s.cfg.Minio.Enabled}
	if !st.Enabled {
		return st
	}
	if s.store == nil {
		st.Error = "client nil"
		return st
	}
	if err := s.store.Ping(ctx); err != nil {
		st.Error = err.Error()
		return st
	}
	st.Connected = true
	return st
}

func (s *Server) checkKafka(ctx context.Context) DepStatus {
	st := DepStatus{Enabled: s.cfg.Kafka.Enabled}
	if !st.Enabled {
		return st
	}
	if s.bus == nil {
		st.Error = "client nil"
		return st
	}
	if err := s.bus.Ping(ctx); err != nil {
		st.Error = err.Error()
		return st
	}
	st.Connected = true
	// 关键：broker 可连不代表消费者在跑；反映真实消费循环状态。
	st.Consuming = s.bus.Consuming()
	if !st.Consuming {
		st.Error = "consumer loop not active (reconnecting)"
	}
	return st
}

// debugJob 本地联调：直接投递一条 JobMessage（异步执行）
func (s *Server) debugJob(c *gin.Context) {
	var msg protocol.JobMessage
	if err := c.ShouldBindJSON(&msg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if msg.SchemaVersion == 0 {
		msg.SchemaVersion = 1
	}
	go func(m protocol.JobMessage) {
		_ = s.runner.Handle(context.Background(), m)
	}(msg)
	c.JSON(http.StatusAccepted, gin.H{"accepted": true, "job_id": msg.JobID})
}
