package minio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"my_transcode/internal/config"
)

const (
	putAttempts = 5
	putBackoff  = 400 * time.Millisecond
)

// Client MinIO 读写封装。
type Client struct {
	cfg    config.MinioConfig
	client *miniogo.Client
}

func New(cfg config.MinioConfig) (*Client, error) {
	c := &Client{cfg: cfg}
	if !cfg.Enabled {
		return c, nil
	}
	cli, err := miniogo.New(cfg.Endpoint, &miniogo.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio new: %w", err)
	}
	c.client = cli
	return c, nil
}

func (c *Client) Enabled() bool { return c.cfg.Enabled }

// Ping 探测连通性（ListBuckets）。
func (c *Client) Ping(ctx context.Context) error {
	if !c.cfg.Enabled {
		return fmt.Errorf("minio disabled")
	}
	if c.client == nil {
		return fmt.Errorf("minio client not initialized")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := c.client.ListBuckets(ctx)
	return err
}

// EnsureBucket 桶不存在则创建。
func (c *Client) EnsureBucket(ctx context.Context, bucket string) error {
	if !c.cfg.Enabled || c.client == nil {
		return fmt.Errorf("minio disabled")
	}
	if bucket == "" {
		bucket = c.cfg.Bucket
	}
	exists, err := c.client.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}
	if !exists {
		if err := c.client.MakeBucket(ctx, bucket, miniogo.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("make bucket %s: %w", bucket, err)
		}
	}
	return nil
}

// Download 下载对象到本地路径。
func (c *Client) Download(ctx context.Context, bucket, key, destPath string) error {
	if !c.cfg.Enabled || c.client == nil {
		return fmt.Errorf("minio disabled")
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	obj, err := c.client.GetObject(ctx, bucket, key, miniogo.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("get object %s/%s: %w", bucket, key, err)
	}
	defer obj.Close()

	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, obj); err != nil {
		return fmt.Errorf("download %s/%s: %w", bucket, key, err)
	}
	return nil
}

// UploadDir 上传本地目录下文件到 prefix（用于 m3u8 + ts）。
func (c *Client) UploadDir(ctx context.Context, bucket, prefix, localDir string) error {
	if !c.cfg.Enabled || c.client == nil {
		return fmt.Errorf("minio disabled")
	}
	if err := c.EnsureBucket(ctx, bucket); err != nil {
		return err
	}
	prefix = strings.TrimLeft(prefix, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	// 递归上传(多码率 HLS 会有 720p/ 480p/ 子目录)。对象 key 保留相对子路径。
	return filepath.WalkDir(localDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(localDir, path)
		if err != nil {
			return err
		}
		objectKey := prefix + filepath.ToSlash(rel)
		return c.putFile(ctx, bucket, objectKey, path)
	})
}

// UploadFile 上传单个本地文件（联调用）。
func (c *Client) UploadFile(ctx context.Context, bucket, key, localPath string) error {
	if !c.cfg.Enabled || c.client == nil {
		return fmt.Errorf("minio disabled")
	}
	if err := c.EnsureBucket(ctx, bucket); err != nil {
		return err
	}
	return c.putFile(ctx, bucket, key, localPath)
}

// putFile 用 FPutObject 每次打开新文件句柄，避免 PutObject 内部重试时 Reader 已读过、
// Content-Length 对不上。瞬时网络错误按文件退避重试，不把整单 HLS 上传打翻。
func (c *Client) putFile(ctx context.Context, bucket, key, localPath string) error {
	opts := miniogo.PutObjectOptions{ContentType: contentTypeOf(localPath)}
	var last error
	for i := 1; i <= putAttempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := c.client.FPutObject(ctx, bucket, key, localPath, opts)
		if err == nil {
			if i > 1 {
				log.Printf("minio: put %s/%s ok on retry %d", bucket, key, i)
			}
			return nil
		}
		last = err
		if !isTransientPutErr(err) || i == putAttempts {
			break
		}
		wait := putBackoff * time.Duration(i)
		log.Printf("minio: put %s/%s failed (attempt %d/%d): %v; retry in %s", bucket, key, i, putAttempts, err, wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return fmt.Errorf("put %s/%s: %w", bucket, key, last)
}

func isTransientPutErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	s := strings.ToLower(err.Error())
	for _, p := range []string{
		"content-length",
		"connection reset",
		"broken pipe",
		"unexpected eof",
		"i/o timeout",
		"tls handshake timeout",
		"slowdown",
		"please try again",
		"connection refused",
		"use of closed network connection",
		"http2: server sent goaway",
		"503",
		"429",
	} {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

// PublicURL 拼公开访问地址。
func (c *Client) PublicURL(bucket, key string) string {
	base := c.cfg.PublicURL
	if base == "" {
		scheme := "http"
		if c.cfg.UseSSL {
			scheme = "https"
		}
		base = fmt.Sprintf("%s://%s", scheme, c.cfg.Endpoint)
	}
	return fmt.Sprintf("%s/%s/%s", trimSlash(base), bucket, strings.TrimLeft(key, "/"))
}

func contentTypeOf(name string) string {
	ext := filepath.Ext(name)
	switch strings.ToLower(ext) {
	case ".m3u8":
		return "application/vnd.apple.mpegurl"
	case ".ts":
		return "video/mp2t"
	case ".mp4":
		return "video/mp4"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return "application/octet-stream"
}

func trimSlash(s string) string {
	return strings.TrimRight(s, "/")
}
