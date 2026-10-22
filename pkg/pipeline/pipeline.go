package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"tg_file_proxy/config"
	"tg_file_proxy/pkg/downloader"
	"tg_file_proxy/pkg/manifest"
	"tg_file_proxy/pkg/telegram"
)

// Orchestrator coordinates the download-to-telegram pipeline.
type Orchestrator struct {
	cfg        *config.Config
	downloader *downloader.Client
	tgClient   telegram.Uploader
}

// NewOrchestrator creates a new pipeline orchestrator.
func NewOrchestrator(cfg *config.Config, tgClient telegram.Uploader) *Orchestrator {
	return &Orchestrator{
		cfg: cfg,
		downloader: downloader.NewClientWithOptions(downloader.ClientOptions{
			Timeout:    time.Duration(cfg.DownloadTimeoutMin) * time.Minute,
			UserAgent:  cfg.DownloadUserAgent,
			MaxRetries: cfg.DownloadMaxRetries,
		}),
		tgClient: tgClient,
	}
}

// ProcessURL processes a single download-and-upload task.
func (o *Orchestrator) ProcessURL(ctx context.Context, fileURL string) (*manifest.Manifest, error) {
	log.Printf("[Pipeline] Probing URL: %s", fileURL)

	info, err := o.downloader.Probe(ctx, fileURL)
	if err != nil {
		return nil, fmt.Errorf("probe failed: %w", err)
	}

	if !info.AcceptRanges {
		return nil, fmt.Errorf("server does not support HTTP Range requests for %s", fileURL)
	}

	if info.Size <= 0 {
		return nil, fmt.Errorf("unknown file size for %s", fileURL)
	}

	taskID := generateTaskID()
	chunkSize := o.cfg.ChunkSizeMB * 1024 * 1024
	totalParts := int((info.Size + chunkSize - 1) / chunkSize)

	log.Printf("[Task %s] File: %s | Size: %.2f MB | Chunks: %d x %d MB",
		taskID, info.Filename, float64(info.Size)/(1024*1024), totalParts, o.cfg.ChunkSizeMB)

	// Send initial status message to Telegram
	statusMsg := fmt.Sprintf("🚀 Task %s started\n📄 File: %s\n📦 Total Size: %.2f MB\n🧩 Chunks: %d (part size: %d MB)",
		taskID, info.Filename, float64(info.Size)/(1024*1024), totalParts, o.cfg.ChunkSizeMB)
	_, _ = o.tgClient.SendTextMessage(ctx, statusMsg)

	m := &manifest.Manifest{
		TaskID:      taskID,
		OriginalURL: fileURL,
		Filename:    info.Filename,
		TotalSize:   info.Size,
		ChunkSize:   chunkSize,
		TotalParts:  totalParts,
		Parts:       make([]manifest.PartInfo, 0, totalParts),
	}

	for i := 0; i < totalParts; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		startByte := int64(i) * chunkSize
		endByte := startByte + chunkSize - 1
		if endByte >= info.Size {
			endByte = info.Size - 1
		}
		partSize := endByte - startByte + 1

		chunkName := fmt.Sprintf("%s.part_%04d", info.Filename, i)
		tempFilePath := filepath.Join(o.cfg.TempDir, fmt.Sprintf("%s_part_%04d.tmp", taskID, i))

		log.Printf("[Task %s] Downloading part %d/%d (%d MB) using %d parallel streams...",
			taskID, i+1, totalParts, partSize/(1024*1024), o.cfg.DownloadConcurrency)

		// 1. Download chunk via parallel HTTP Range requests
		md5Hash, err := o.downloader.DownloadRangeParallel(
			ctx,
			fileURL,
			startByte,
			endByte,
			tempFilePath,
			o.cfg.DownloadConcurrency,
			nil,
		)
		if err != nil {
			_ = os.Remove(tempFilePath)
			return nil, fmt.Errorf("failed downloading part %d: %w", i, err)
		}

		log.Printf("[Task %s] Part %d/%d downloaded (MD5: %s). Uploading to Telegram...",
			taskID, i+1, totalParts, md5Hash)

		// 2. Upload chunk to Telegram via MTProto
		caption := fmt.Sprintf("#task_%s #part_%04d_of_%04d\n%s", taskID, i+1, totalParts, chunkName)
		msgID, err := o.tgClient.UploadChunk(ctx, tempFilePath, chunkName, caption)

		// 3. CRITICAL: Remove temp file immediately to keep VPS disk free!
		_ = os.Remove(tempFilePath)

		if err != nil {
			return nil, fmt.Errorf("failed uploading part %d: %w", i, err)
		}

		log.Printf("[Task %s] Part %d/%d successfully uploaded (MsgID: %d). Disk chunk removed.",
			taskID, i+1, totalParts, msgID)

		m.Parts = append(m.Parts, manifest.PartInfo{
			Index:     i,
			StartByte: startByte,
			EndByte:   endByte,
			Size:      partSize,
			MD5:       md5Hash,
			MsgID:     msgID,
		})
	}

	// 4. Send Manifest JSON to Telegram
	manifestBytes, _ := json.MarshalIndent(m, "", "  ")
	manifestText := fmt.Sprintf("✅ Task %s COMPLETE!\n\n```json\n%s\n```", taskID, string(manifestBytes))
	_, _ = o.tgClient.SendTextMessage(ctx, manifestText)

	// Also save manifest locally in TempDir for easy access
	_ = m.SaveToFile(filepath.Join(o.cfg.TempDir, fmt.Sprintf("%s_manifest.json", taskID)))

	log.Printf("[Task %s] All %d parts successfully processed and uploaded!", taskID, totalParts)
	return m, nil
}

func generateTaskID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())[:8]
	}
	return hex.EncodeToString(b)
}
