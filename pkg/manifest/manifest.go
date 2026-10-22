package manifest

import (
	"encoding/json"
	"os"
)

// PartInfo holds metadata about a single uploaded chunk.
type PartInfo struct {
	Index     int    `json:"index"`
	StartByte int64  `json:"start_byte"`
	EndByte   int64  `json:"end_byte"`
	Size      int64  `json:"size"`
	MD5       string `json:"md5"`
	MsgID     int    `json:"msg_id,omitempty"`
	FileID    string `json:"file_id,omitempty"`
}

// Manifest represents the complete metadata for a chunked file transfer.
type Manifest struct {
	TaskID      string     `json:"task_id"`
	OriginalURL string     `json:"original_url"`
	Filename    string     `json:"filename"`
	TotalSize   int64      `json:"total_size"`
	ChunkSize   int64      `json:"chunk_size"`
	TotalParts  int        `json:"total_parts"`
	FileMD5     string     `json:"file_md5,omitempty"`
	Parts       []PartInfo `json:"parts"`
}

// SaveToFile writes the manifest to a JSON file.
func (m *Manifest) SaveToFile(path string) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// LoadFromFile reads a manifest from a JSON file.
func LoadFromFile(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
