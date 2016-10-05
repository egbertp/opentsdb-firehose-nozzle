package poster

type Tags struct {
	Deployment string `json:"deployment"`
	Job        string `json:"job"`
	Index      string `json:"index"`
	IP         string `json:"ip"`
}

type Metric struct {
	Metric    string  `json:"metric"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Tags      Tags    `json:"tags"`
}
