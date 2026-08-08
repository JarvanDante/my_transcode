package protocol

// Topics — 与 my_service 约定一致
const (
	TopicJobs    = "media.transcode.jobs"
	TopicResults = "media.transcode.results"
)

// Status
const (
	StatusProcessing = "processing"
	StatusReady      = "ready"
	StatusFailed     = "failed"
)

// Profile 第一期只支持 H.264 HLS
const ProfileH264HLS = "h264_hls"

// ObjectRef MinIO 对象定位
type ObjectRef struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// OutputRef HLS 输出目录前缀
type OutputRef struct {
	Bucket string `json:"bucket"`
	Prefix string `json:"prefix"` // 如 my/hls/2/ ，最终 play_key = prefix + "index.m3u8"
}

// JobMessage 转码任务（Kafka / debug HTTP）
type JobMessage struct {
	SchemaVersion int       `json:"schema_version"`
	JobID         string    `json:"job_id"`
	Biz           string    `json:"biz"`
	BizRef        string    `json:"biz_ref"`
	Input         ObjectRef `json:"input"`
	Output        OutputRef `json:"output"`
	Profile       string    `json:"profile"`
	// CoverSeekSec 封面截取秒数；<=0 时用 worker 配置默认值
	CoverSeekSec int    `json:"cover_seek_sec,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
}

// ResultMessage 转码结果
type ResultMessage struct {
	SchemaVersion int    `json:"schema_version"`
	JobID         string `json:"job_id"`
	Biz           string `json:"biz"`
	BizRef        string `json:"biz_ref"`
	Status        string `json:"status"`
	PlayKey       string `json:"play_key,omitempty"`
	PlayURL       string `json:"play_url,omitempty"`
	CoverKey      string `json:"cover_key,omitempty"`
	CoverURL      string `json:"cover_url,omitempty"`
	DurationSec   int    `json:"duration_sec,omitempty"`
	Error         string `json:"error,omitempty"`
	FinishedAt    string `json:"finished_at,omitempty"`
}
