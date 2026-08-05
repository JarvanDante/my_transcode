package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	HTTP    HTTPConfig   `yaml:"http"`
	WorkDir string       `yaml:"workDir"`
	Kafka   KafkaConfig  `yaml:"kafka"`
	Minio   MinioConfig  `yaml:"minio"`
	FFmpeg  FFmpegConfig `yaml:"ffmpeg"`
	Debug   DebugConfig  `yaml:"debug"`
}

type HTTPConfig struct {
	Addr string `yaml:"addr"`
}

type KafkaConfig struct {
	Enabled     bool     `yaml:"enabled"`
	Brokers     []string `yaml:"brokers"`
	GroupID     string   `yaml:"groupId"`
	JobTopic    string   `yaml:"jobTopic"`
	ResultTopic string   `yaml:"resultTopic"`
}

type MinioConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"accessKey"`
	SecretKey string `yaml:"secretKey"`
	UseSSL    bool   `yaml:"useSSL"`
	Bucket    string `yaml:"bucket"`
	PublicURL string `yaml:"publicURL"`
}

type FFmpegConfig struct {
	Bin     string `yaml:"bin"`
	Profile string `yaml:"profile"`
	Preset  string `yaml:"preset"`
	CRF     int    `yaml:"crf"`
	HLSTime int    `yaml:"hlsTime"`
}

type DebugConfig struct {
	AllowSubmit bool `yaml:"allowSubmit"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if c.HTTP.Addr == "" {
		c.HTTP.Addr = ":8088"
	}
	if c.WorkDir == "" {
		c.WorkDir = "/tmp/my_transcode"
	}
	if c.FFmpeg.Bin == "" {
		c.FFmpeg.Bin = "ffmpeg"
	}
	if c.FFmpeg.Preset == "" {
		c.FFmpeg.Preset = "veryfast"
	}
	if c.FFmpeg.CRF == 0 {
		c.FFmpeg.CRF = 23
	}
	if c.FFmpeg.HLSTime == 0 {
		c.FFmpeg.HLSTime = 6
	}
	return &c, nil
}
