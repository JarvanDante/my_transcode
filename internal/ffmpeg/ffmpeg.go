package ffmpeg

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"my_transcode/internal/config"
)

// Transcoder 封装 ffmpeg 命令（第一期：H.264 + HLS）
type Transcoder struct {
	cfg config.FFmpegConfig
}

func New(cfg config.FFmpegConfig) *Transcoder {
	return &Transcoder{cfg: cfg}
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
