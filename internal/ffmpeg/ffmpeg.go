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

// ExtractCover 从视频 seekSec 处截一帧 JPEG。
func (t *Transcoder) ExtractCover(ctx context.Context, inputFile, outFile string, seekSec int) error {
	if seekSec < 0 {
		seekSec = 0
	}
	if err := os.MkdirAll(filepath.Dir(outFile), 0o755); err != nil {
		return err
	}
	args := []string{
		"-y",
		"-ss", fmt.Sprintf("%d", seekSec),
		"-i", inputFile,
		"-frames:v", "1",
		"-q:v", "2",
		outFile,
	}
	cmd := exec.CommandContext(ctx, t.cfg.Bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffmpeg cover: %w", err)
	}
	return nil
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

// ---- M3-1 多码率 HLS ----

// Rendition ABR 档位定义。
type Rendition struct {
	Name        string // 子目录名/清单名, 如 "720p"
	Height      int
	VideoKbps   int
	MaxrateKbps int
	BufKbps     int
	AudioKbps   int
}

// RenditionOut 产出档位信息(用于生成 master.m3u8)。
type RenditionOut struct {
	Name      string
	Bandwidth int // bps, master BANDWIDTH(峰值≈maxrate+audio)
	Width     int // 0 表示未知(探测失败)
	Height    int
	Playlist  string // 相对 master 的清单路径, 如 "720p/index.m3u8"
}

// Probe 读取首个视频流的宽高。失败返回 0,0。
func (t *Transcoder) Probe(ctx context.Context, inputFile string) (w, h int) {
	args := []string{
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "stream=width,height",
		"-of", "csv=s=x:p=0",
		inputFile,
	}
	cmd := exec.CommandContext(ctx, t.probeBin(), args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return 0, 0
	}
	s := strings.TrimSpace(out.String())
	if idx := strings.IndexAny(s, "\r\n"); idx >= 0 {
		s = s[:idx]
	}
	parts := strings.Split(s, "x")
	if len(parts) != 2 {
		return 0, 0
	}
	w, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
	h, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
	if w < 0 || h < 0 {
		return 0, 0
	}
	return w, h
}

// DefaultLadder 按源高度筛选档位(高→低)。原片低于某档位则跳过；
// 源低于最小档位(480)时产出「源分辨率」单档，避免放大浪费。
func DefaultLadder(srcH int) []Rendition {
	full := []Rendition{
		{Name: "720p", Height: 720, VideoKbps: 2800, MaxrateKbps: 3000, BufKbps: 4200, AudioKbps: 128},
		{Name: "480p", Height: 480, VideoKbps: 1400, MaxrateKbps: 1500, BufKbps: 2100, AudioKbps: 128},
	}
	if srcH <= 0 {
		return full
	}
	var out []Rendition
	for _, r := range full {
		if r.Height <= srcH+8 { // 容差; 原片低于档位则跳过
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		out = []Rendition{{
			Name: fmt.Sprintf("%dp", srcH), Height: srcH,
			VideoKbps: 1200, MaxrateKbps: 1400, BufKbps: 1800, AudioKbps: 128,
		}}
	}
	return out
}

// ToHLSMulti 生成多码率 HLS:
//
//	outDir/<name>/index.m3u8 + index*.ts (每档)
//	outDir/master.m3u8       (主清单, 相对引用各档)
//
// 段 URI 保持相对(index0.ts / 720p/index.m3u8)，网关按需重写 token。
func (t *Transcoder) ToHLSMulti(ctx context.Context, inputFile, outDir string, srcW, srcH int, rends []Rendition) ([]RenditionOut, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	var outs []RenditionOut
	for _, r := range rends {
		sub := filepath.Join(outDir, r.Name)
		if err := os.MkdirAll(sub, 0o755); err != nil {
			return nil, err
		}
		playlist := filepath.Join(sub, "index.m3u8")
		args := []string{
			"-y", "-i", inputFile,
			"-vf", fmt.Sprintf("scale=-2:%d", r.Height),
			"-c:v", "libx264",
			"-preset", t.cfg.Preset,
			"-b:v", fmt.Sprintf("%dk", r.VideoKbps),
			"-maxrate", fmt.Sprintf("%dk", r.MaxrateKbps),
			"-bufsize", fmt.Sprintf("%dk", r.BufKbps),
			"-c:a", "aac", "-b:a", fmt.Sprintf("%dk", r.AudioKbps),
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
			return nil, fmt.Errorf("ffmpeg %s: %w", r.Name, err)
		}
		width := 0
		if srcW > 0 && srcH > 0 {
			width = srcW * r.Height / srcH
			if width%2 != 0 {
				width++
			}
		}
		outs = append(outs, RenditionOut{
			Name:      r.Name,
			Bandwidth: (r.MaxrateKbps + r.AudioKbps) * 1000,
			Width:     width,
			Height:    r.Height,
			Playlist:  r.Name + "/index.m3u8",
		})
	}
	if err := writeMaster(filepath.Join(outDir, "master.m3u8"), outs); err != nil {
		return nil, err
	}
	return outs, nil
}

func writeMaster(path string, outs []RenditionOut) error {
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n")
	for _, o := range outs {
		b.WriteString("#EXT-X-STREAM-INF:BANDWIDTH=")
		b.WriteString(strconv.Itoa(o.Bandwidth))
		if o.Width > 0 && o.Height > 0 {
			fmt.Fprintf(&b, ",RESOLUTION=%dx%d", o.Width, o.Height)
		}
		fmt.Fprintf(&b, ",NAME=%q\n", o.Name)
		b.WriteString(o.Playlist)
		b.WriteString("\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
