package job

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"my_transcode/internal/aesbnc"
	"my_transcode/internal/config"
	"my_transcode/internal/ffmpeg"
	"my_transcode/internal/minio"
	"my_transcode/internal/protocol"
)

// ResultPublisher 回写结果（Kafka 或仅日志）
type ResultPublisher interface {
	PublishResult(ctx context.Context, msg protocol.ResultMessage) error
}

// Runner 编排：下载 → ffmpeg → 上传 → 回执
type Runner struct {
	cfg   *config.Config
	store *minio.Client
	ff    *ffmpeg.Transcoder
	pub   ResultPublisher
	seen  sync.Map // job_id → struct{}，进程内去重，挡住 Kafka 重投
}

func New(cfg *config.Config, store *minio.Client, ff *ffmpeg.Transcoder, pub ResultPublisher) *Runner {
	return &Runner{cfg: cfg, store: store, ff: ff, pub: pub}
}

func (r *Runner) Handle(ctx context.Context, job protocol.JobMessage) error {
	if job.JobID != "" {
		if _, loaded := r.seen.LoadOrStore(job.JobID, struct{}{}); loaded {
			log.Printf("job skip duplicate id=%s (already handled in this process)", job.JobID)
			return nil
		}
	}
	log.Printf("job start id=%s ref=%s key=%s", job.JobID, job.BizRef, job.Input.Key)

	result := protocol.ResultMessage{
		SchemaVersion: 1,
		JobID:         job.JobID,
		Biz:           job.Biz,
		BizRef:        job.BizRef,
		Status:        protocol.StatusFailed,
	}

	if err := r.run(ctx, job, &result); err != nil {
		result.Status = protocol.StatusFailed
		result.Error = err.Error()
		result.FinishedAt = time.Now().Format(time.RFC3339)
		_ = r.publish(ctx, result)
		return err
	}

	result.Status = protocol.StatusReady
	result.FinishedAt = time.Now().Format(time.RFC3339)
	return r.publish(ctx, result)
}

func (r *Runner) run(ctx context.Context, job protocol.JobMessage, result *protocol.ResultMessage) error {
	if job.JobID == "" {
		return fmt.Errorf("job_id required")
	}
	profile := job.Profile
	if profile == "" {
		profile = protocol.ProfileH264HLS
	}
	if profile != protocol.ProfileH264HLS {
		return fmt.Errorf("unsupported profile: %s", profile)
	}

	work := filepath.Join(r.cfg.WorkDir, job.JobID)
	inFile := filepath.Join(work, "source"+extOf(job.Input.Key))
	outDir := filepath.Join(work, "hls")
	defer os.RemoveAll(work)

	if err := os.MkdirAll(work, 0o755); err != nil {
		return err
	}

	bucketIn := job.Input.Bucket
	if bucketIn == "" {
		bucketIn = r.cfg.Minio.Bucket
	}
	bucketOut := job.Output.Bucket
	if bucketOut == "" {
		bucketOut = r.cfg.Minio.Bucket
	}
	prefix := job.Output.Prefix
	if prefix == "" {
		return fmt.Errorf("output.prefix required")
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	if r.store == nil || !r.store.Enabled() {
		return fmt.Errorf("minio disabled; enable minio to run full pipeline (or use scripts/transcode_local.sh)")
	}

	if err := r.store.Download(ctx, bucketIn, job.Input.Key, inFile); err != nil {
		return err
	}
	if dur := r.ff.DurationSec(ctx, inFile); dur > 0 {
		result.DurationSec = dur
		log.Printf("job id=%s duration_sec=%d", job.JobID, dur)
	}
	srcW, srcH := r.ff.Probe(ctx, inFile)
	ladder := ffmpeg.DefaultLadder(srcH)
	outs, err := r.ff.ToHLSMulti(ctx, inFile, outDir, srcW, srcH, ladder)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(outs))
	for _, o := range outs {
		names = append(names, o.Name)
	}
	log.Printf("job id=%s src=%dx%d renditions=%v", job.JobID, srcW, srcH, names)

	seek := job.CoverSeekSec
	if seek <= 0 {
		seek = r.cfg.FFmpeg.CoverSeekSec
	}
	if seek < 0 {
		seek = 0
	}
	if result.DurationSec > 0 && seek >= result.DurationSec {
		seek = result.DurationSec / 2
	}
	coverFile := filepath.Join(outDir, "cover.jpg")
	if err := r.ff.ExtractCover(ctx, inFile, coverFile, seek); err != nil {
		log.Printf("job id=%s cover extract failed seek=%d: %v (continue without cover)", job.JobID, seek, err)
	} else {
		log.Printf("job id=%s cover extracted seek=%d", job.JobID, seek)
	}

	if _, err := os.Stat(coverFile); err == nil {
		plain, err := os.ReadFile(coverFile)
		if err != nil {
			log.Printf("job id=%s cover read failed: %v", job.JobID, err)
		} else if enc, err := aesbnc.Encrypt(plain); err != nil {
			log.Printf("job id=%s cover encrypt failed: %v", job.JobID, err)
		} else if err := os.WriteFile(filepath.Join(outDir, "cover.bnc"), enc, 0644); err != nil {
			log.Printf("job id=%s cover.bnc write failed: %v", job.JobID, err)
		} else if err := os.Remove(coverFile); err != nil {
			log.Printf("job id=%s cover.jpg remove failed: %v", job.JobID, err)
		}
	}

	if err := r.store.UploadDir(ctx, bucketOut, prefix, outDir); err != nil {
		return err
	}

	playKey := prefix + "master.m3u8"
	result.PlayKey = playKey
	result.PlayURL = r.store.PublicURL(bucketOut, playKey)
	if _, err := os.Stat(filepath.Join(outDir, "cover.bnc")); err == nil {
		coverKey := prefix + "cover.bnc"
		result.CoverKey = coverKey
		result.CoverURL = r.store.PublicURL(bucketOut, coverKey)
	}
	return nil
}

func (r *Runner) publish(ctx context.Context, msg protocol.ResultMessage) error {
	if r.pub == nil {
		log.Printf("job result (no publisher): %+v", msg)
		return nil
	}
	if err := r.pub.PublishResult(ctx, msg); err != nil {
		log.Printf("publish result failed: %v; payload=%+v", err, msg)
		return err
	}
	return nil
}

func extOf(key string) string {
	ext := filepath.Ext(key)
	if ext == "" {
		return ".mp4"
	}
	return ext
}
