package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"my_transcode/internal/config"
	"my_transcode/internal/job"
	"my_transcode/internal/protocol"
)

type Server struct {
	cfg    *config.Config
	runner *job.Runner
	engine *gin.Engine
}

func New(cfg *config.Config, runner *job.Runner) *Server {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), gin.Logger())

	s := &Server{cfg: cfg, runner: runner, engine: r}
	r.GET("/healthz", s.healthz)
	if cfg.Debug.AllowSubmit {
		r.POST("/debug/jobs", s.debugJob)
	}
	return s
}

func (s *Server) Engine() *gin.Engine { return s.engine }

func (s *Server) healthz(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"service": "my_transcode",
		"time":    time.Now().Format(time.RFC3339),
		"kafka":   s.cfg.Kafka.Enabled,
		"minio":   s.cfg.Minio.Enabled,
	})
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
