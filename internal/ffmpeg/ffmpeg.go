package ffmpeg

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"my_transcode/internal/config"
)

// Transcoder 封装 ffmpeg 命令（第一期：H.264 + HLS）
type Transcoder struct {
	cfg config.FFmpegConfig
}

func New(cfg config.FFmpegConfig) *Transcoder {
	return &Transcoder{cfg: cfg}
}

func (t *Transcoder) probeBin() string {
	bin := t.cfg.Bin
	if bin == "" || bin == "ffmpeg" {
		return "ffprobe"
	}
	if strings.HasSuffix(bin, "ffmpeg") {
		return strings.TrimSuffix(bin, "ffmpeg") + "ffprobe"
	}
	return "ffprobe"
}

// DurationSec 用 ffprobe 读取媒体时长(秒, 四舍五入)。失败返回 0。
func (t *Transcoder) DurationSec(ctx context.Context, inputFile string) int {
	args := []string{
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		inputFile,
	}
	cmd := exec.CommandContext(ctx, t.probeBin(), args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return 0
	}
	s := strings.TrimSpace(out.String())
	if s == "" {
		return 0
	}
	sec, err := strconv.ParseFloat(s, 64)
	if err != nil || sec <= 0 {
		return 0
	}
	return int(math.Round(sec))
}

// ToHLS 将 inputFile 转为 outDir/index.m3u8 + ts
func (t *Transcoder) ToHLS(ctx context.Context, inputFile, outDir string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	playlist := filepath.Join(outDir, "index.m3u8")
	args := []string{
		"-y", "-i", inputFile,
		"-c:v", "libx264",
		"-preset", t.cfg.Preset,
		"-crf", fmt.Sprintf("%d", t.cfg.CRF),
		"-c:a", "aac", "-b:a", "128k",
		"-hls_time", fmt.Sprintf("%d", t.cfg.HLSTime),
		"-hls_list_size", "0",
		"-hls_playlist_type", "vod",
		"-f", "hls",
		playlist,
	}
	cmd := exec.CommandContext(ctx, t.cfg.Bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffmpeg: %w", err)
	}
	return nil
}
