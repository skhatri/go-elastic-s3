package model

type ElasticS3Config struct {
	Input         string              `json:"input"`
	ElasticSearch ElasticSearchConfig `json:"elasticsearch"`
	S3            S3Config            `json:"s3"`
	Tasks         TasksConfig         `json:"tasks"`
	Output        OutputConfig        `json:"output"`
	Upload        UploadConfig        `json:"upload"`
}

type OutputConfig struct {
	TmpFolder string `json:"tmp-folder"`
}

type TransformField struct {
	Name      string `json:"name"`
	Operation string `json:"operation"`
}

type UploadTransformConfig struct {
	Fields []TransformField `json:"fields"`
}

type UploadConfig struct {
	Transform UploadTransformConfig `json:"transform"`
}

type ElasticSearchConfig struct {
	Host  string `json:"host"`
	Index string `json:"index"`
	Key   string `json:"key"`
	Username *string `json:"username"`
	Password *string `json:"password"`
}

type TasksConfig struct {
	Index    bool `json:"index"`
	Upload   bool `json:"upload"`
	Dump     bool `json:"dump"`
	Recreate bool `json:"recreate"`
}

type S3Config struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type ElasticsearchResponse struct {
	Hits HitsConfig `json:"hits"`
}

type HitsConfig struct {
	Total TotalConfig `json:"total"`
	Hits  []HitsData  `json:"hits"`
}

type TotalConfig struct {
	Value int `json:"value"`
}
type HitsData struct {
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	Source map[string]interface{} `json:"_source"`
}
