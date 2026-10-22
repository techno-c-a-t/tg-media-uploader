package telegram

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
)

// Uploader defines the interface for sending messages and files to Telegram.
type Uploader interface {
	SendTextMessage(ctx context.Context, text string) (int, error)
	UploadChunk(ctx context.Context, filePath, filename, caption string, onProgress func(uploaded, total int64)) (int, error)
}

// MockUploader simulates Telegram uploads for local testing without real credentials.
type MockUploader struct {
	OutputDir string
	counter   int
}

// NewMockUploader creates a mock uploader that stores uploaded chunks in outputDir.
func NewMockUploader(outputDir string) *MockUploader {
	if outputDir != "" {
		_ = os.MkdirAll(outputDir, 0755)
	}
	return &MockUploader{OutputDir: outputDir}
}

func (m *MockUploader) SendTextMessage(ctx context.Context, text string) (int, error) {
	log.Printf("[MockTG Message]\n%s", text)
	m.counter++
	return m.counter, nil
}

func (m *MockUploader) UploadChunk(ctx context.Context, filePath, filename, caption string, onProgress func(uploaded, total int64)) (int, error) {
	log.Printf("[MockTG Upload] Saving chunk %s to simulated channel %s...", filename, m.OutputDir)
	if m.OutputDir != "" {
		dest := filepath.Join(m.OutputDir, filename)
		srcFile, err := os.Open(filePath)
		if err != nil {
			return 0, err
		}
		defer srcFile.Close()

		dstFile, err := os.Create(dest)
		if err != nil {
			return 0, err
		}
		defer dstFile.Close()

		if _, err := io.Copy(dstFile, srcFile); err != nil {
			return 0, err
		}
	}
	m.counter++
	return m.counter, nil
}
