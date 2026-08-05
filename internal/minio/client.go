package minio

import (
	"context"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"my_transcode/internal/config"
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

	entries, err := os.ReadDir(localDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		localPath := filepath.Join(localDir, name)
		objectKey := prefix + name
		f, err := os.Open(localPath)
		if err != nil {
			return err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		_, err = c.client.PutObject(ctx, bucket, objectKey, f, st.Size(), miniogo.PutObjectOptions{
			ContentType: contentTypeOf(name),
		})
		f.Close()
		if err != nil {
			return fmt.Errorf("put %s/%s: %w", bucket, objectKey, err)
		}
	}
	return nil
}

// UploadFile 上传单个本地文件（联调用）。
func (c *Client) UploadFile(ctx context.Context, bucket, key, localPath string) error {
	if !c.cfg.Enabled || c.client == nil {
		return fmt.Errorf("minio disabled")
	}
	if err := c.EnsureBucket(ctx, bucket); err != nil {
		return err
	}
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	_, err = c.client.PutObject(ctx, bucket, key, f, st.Size(), miniogo.PutObjectOptions{
		ContentType: contentTypeOf(localPath),
	})
	return err
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
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return "application/octet-stream"
}

func trimSlash(s string) string {
	return strings.TrimRight(s, "/")
}
