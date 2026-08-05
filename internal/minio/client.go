package minio

import (
	"context"
	"fmt"

	"my_transcode/internal/config"
)

// Client 对象存储骨架。第 4 步再接官方 minio-go SDK。
type Client struct {
	cfg config.MinioConfig
}

func New(cfg config.MinioConfig) *Client {
	return &Client{cfg: cfg}
}

func (c *Client) Enabled() bool { return c.cfg.Enabled }

// Download 下载对象到本地路径。
func (c *Client) Download(ctx context.Context, bucket, key, destPath string) error {
	_ = ctx
	_ = bucket
	_ = key
	_ = destPath
	return fmt.Errorf("minio: Download not implemented yet (enable after wiring minio-go)")
}

// UploadDir 上传本地目录下文件到 prefix（用于 m3u8 + ts）。
func (c *Client) UploadDir(ctx context.Context, bucket, prefix, localDir string) error {
	_ = ctx
	_ = bucket
	_ = prefix
	_ = localDir
	return fmt.Errorf("minio: UploadDir not implemented yet")
}

// PublicURL 拼公开访问地址。
func (c *Client) PublicURL(bucket, key string) string {
	base := c.cfg.PublicURL
	if base == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s", trimSlash(base), bucket, key)
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
