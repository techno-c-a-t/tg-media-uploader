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
	"strings"
	"sync"
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

// ProcessURL processes a single download-and-upload task with comprehensive tracing and live progress.
func (o *Orchestrator) ProcessURL(ctx context.Context, fileURL string) (*manifest.Manifest, error) {
	taskID := generateTaskID()
	overallStartTime := time.Now()

	log.Printf("[Task %s] 🔍 Probing remote URL: %s", taskID, fileURL)

	probeStart := time.Now()
	info, err := o.downloader.Probe(ctx, fileURL)
	if err != nil {
		log.Printf("[Task %s] ❌ Probe failed after %v: %v", taskID, time.Since(probeStart).Round(time.Millisecond), err)
		return nil, fmt.Errorf("probe failed: %w", err)
	}

	log.Printf("[Task %s] 📋 Probe result in %v: filename='%s', size=%.2f MB (%d bytes), acceptRanges=%v",
		taskID, time.Since(probeStart).Round(time.Millisecond), info.Filename,
		float64(info.Size)/(1024*1024), info.Size, info.AcceptRanges)

	if !info.AcceptRanges {
		return nil, fmt.Errorf("server does not support HTTP Range requests for %s", fileURL)
	}

	if info.Size <= 0 {
		return nil, fmt.Errorf("unknown file size for %s", fileURL)
	}

	chunkSize := o.cfg.ChunkSizeMB * 1024 * 1024
	totalParts := int((info.Size + chunkSize - 1) / chunkSize)

	log.Printf("[Task %s] 🚀 Task Initialized: File='%s' | Total Size=%.2f MB | Chunks=%d x %d MB | Streams=%d",
		taskID, info.Filename, float64(info.Size)/(1024*1024), totalParts, o.cfg.ChunkSizeMB, o.cfg.DownloadConcurrency)

	// Send initial status message to Telegram
	statusMsg := fmt.Sprintf("🚀 Задача %s начата\n📄 Файл: `%s`\n📦 Размер: %.2f MB\n🧩 Частей: %d (по %d MB)",
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

		// ---------------------------------------------------------------------
		// STEP 1: PARALLEL SEGMENTED DOWNLOAD (Origin -> VPS)
		// ---------------------------------------------------------------------
		downTracker := newProgressTracker(taskID, "Downloader", i+1, totalParts, partSize)
		log.Printf("[Task %s] [Downloader] ⏳ Starting Part %d/%d (%.2f MB, bytes %d-%d) using %d parallel streams...",
			taskID, i+1, totalParts, float64(partSize)/(1024*1024), startByte, endByte, o.cfg.DownloadConcurrency)

		downStart := time.Now()
		md5Hash, err := o.downloader.DownloadRangeParallel(
			ctx,
			fileURL,
			startByte,
			endByte,
			tempFilePath,
			o.cfg.DownloadConcurrency,
			func(written, total int64, t *downloader.DownloadTelemetry) {
				downTracker.UpdateWithTelemetry(written, t)
			},
		)
		if err != nil {
			_ = os.Remove(tempFilePath)
			log.Printf("[Task %s] [Downloader] ❌ Error downloading part %d/%d after %v: %v",
				taskID, i+1, totalParts, time.Since(downStart).Round(100*time.Millisecond), err)
			return nil, fmt.Errorf("failed downloading part %d: %w", i+1, err)
		}

		downDuration := time.Since(downStart)
		downSpeedMBs := (float64(partSize) / (1024 * 1024)) / downDuration.Seconds()
		downTracker.Finish(fmt.Sprintf(", Speed: %.2f MB/s, MD5: %s", downSpeedMBs, md5Hash))

		// ---------------------------------------------------------------------
		// STEP 2: TELEGRAM MTPROTO UPLOAD (VPS -> Telegram Channel)
		// ---------------------------------------------------------------------
		upTracker := newProgressTracker(taskID, "Telegram", i+1, totalParts, partSize)
		log.Printf("[Task %s] [Telegram] ⏳ Uploading Part %d/%d (%.2f MB, blocks: %d KB, threads: %d)...",
			taskID, i+1, totalParts, float64(partSize)/(1024*1024), o.cfg.UploadPartSizeKB, o.cfg.UploadThreads)

		upStart := time.Now()
		caption := fmt.Sprintf("#task_%s #part_%04d_of_%04d\n%s", taskID, i+1, totalParts, chunkName)
		msgID, err := o.tgClient.UploadChunk(ctx, tempFilePath, chunkName, caption, func(uploaded, total int64) {
			upTracker.Update(uploaded)
		})

		// ---------------------------------------------------------------------
		// STEP 3: IMMEDIATE DISK CLEANUP (Strict FIFO chunk buffer)
		// ---------------------------------------------------------------------
		_ = os.Remove(tempFilePath)

		if err != nil {
			log.Printf("[Task %s] [Telegram] ❌ Error uploading part %d/%d after %v: %v",
				taskID, i+1, totalParts, time.Since(upStart).Round(100*time.Millisecond), err)
			return nil, fmt.Errorf("failed uploading part %d: %w", i+1, err)
		}

		upDuration := time.Since(upStart)
		upSpeedMBs := (float64(partSize) / (1024 * 1024)) / upDuration.Seconds()
		upTracker.Finish(fmt.Sprintf(", Speed: %.2f MB/s, MsgID: %d. Disk chunk purged", upSpeedMBs, msgID))

		// Telegram channel notification per part
		partDoneMsg := fmt.Sprintf("🧩 Часть %d/%d (%.1f MB) отправлена (скачано: %.1f MB/s, залито: %.1f MB/s)",
			i+1, totalParts, float64(partSize)/(1024*1024), downSpeedMBs, upSpeedMBs)
		_, _ = o.tgClient.SendTextMessage(ctx, partDoneMsg)

		m.Parts = append(m.Parts, manifest.PartInfo{
			Index:     i,
			StartByte: startByte,
			EndByte:   endByte,
			Size:      partSize,
			MD5:       md5Hash,
			MsgID:     msgID,
		})
	}

	// -------------------------------------------------------------------------
	// STEP 4: TASK COMPLETION & MANIFEST PUBLISHING
	// -------------------------------------------------------------------------
	overallDuration := time.Since(overallStartTime)
	overallSpeedMBs := (float64(info.Size) / (1024 * 1024)) / overallDuration.Seconds()

	log.Printf("[Task %s] 🎉 SUCCESS! All %d parts completed in %v! Pipeline throughput: %.2f MB/s",
		taskID, totalParts, overallDuration.Round(100*time.Millisecond), overallSpeedMBs)

	manifestBytes, _ := json.MarshalIndent(m, "", "  ")
	manifestText := fmt.Sprintf("✅ Задача %s ПОЛНОСТЬЮ ВЫПОЛНЕНА!\n📄 Файл: `%s`\n⏱ Общее время: %v (средняя скорость: %.1f MB/s)\n\n```json\n%s\n```",
		taskID, info.Filename, overallDuration.Round(time.Second), overallSpeedMBs, string(manifestBytes))
	_, _ = o.tgClient.SendTextMessage(ctx, manifestText)

	// Save manifest locally in TempDir
	_ = m.SaveToFile(filepath.Join(o.cfg.TempDir, fmt.Sprintf("%s_manifest.json", taskID)))

	return m, nil
}

// ProgressTracker manages throttled progress logging with live transfer speed and ETA calculations.
type ProgressTracker struct {
	taskID     string
	stage      string
	partIdx    int
	totalParts int
	totalBytes int64
	startTime  time.Time
	lastLog    time.Time
	mu         sync.Mutex
}

func newProgressTracker(taskID, stage string, partIdx, totalParts int, totalBytes int64) *ProgressTracker {
	now := time.Now()
	return &ProgressTracker{
		taskID:     taskID,
		stage:      stage,
		partIdx:    partIdx,
		totalParts: totalParts,
		totalBytes: totalBytes,
		startTime:  now,
		lastLog:    now,
	}
}

func (p *ProgressTracker) Update(written int64) {
	p.UpdateWithTelemetry(written, nil)
}

func (p *ProgressTracker) UpdateWithTelemetry(written int64, t *downloader.DownloadTelemetry) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	// Throttle logs to every 2 seconds to keep logs clean and readable
	if now.Sub(p.lastLog) < 2*time.Second {
		return
	}

	elapsed := now.Sub(p.startTime).Seconds()
	if elapsed <= 0 {
		return
	}

	speedMBs := (float64(written) / (1024 * 1024)) / elapsed
	pct := float64(written) / float64(p.totalBytes) * 100.0

	var etaStr string
	remainingBytes := p.totalBytes - written
	if speedMBs > 0 && remainingBytes > 0 {
		etaSec := float64(remainingBytes) / (speedMBs * 1024 * 1024)
		etaStr = fmt.Sprintf("ETA: %.0fs", etaSec)
	} else {
		etaStr = "ETA: --"
	}

	log.Printf("[Task %s] [%s] Part %d/%d: %.1f MB / %.1f MB (%.1f%%) | Speed: %.1f MB/s | %s",
		p.taskID, p.stage, p.partIdx, p.totalParts,
		float64(written)/(1024*1024), float64(p.totalBytes)/(1024*1024),
		pct, speedMBs, etaStr)

	if t != nil && len(t.Streams) > 0 {
		var activeCount int
		var streamParts []string
		for _, s := range t.Streams {
			if s.Total > 0 {
				sPct := float64(s.Written) / float64(s.Total) * 100.0
				if s.Status == downloader.StatusStreaming || s.Status == downloader.StatusConnecting {
					activeCount++
				}
				streamParts = append(streamParts, fmt.Sprintf("W#%d: B#%d %.0f%% [%s]", s.ID, s.CurrentBlock, sPct, s.Status))
			} else {
				streamParts = append(streamParts, fmt.Sprintf("W#%d: [%s]", s.ID, s.Status))
			}
		}

		blocksStr := ""
		if t.TotalBlocks > 0 {
			blocksStr = fmt.Sprintf(" | Blocks: %d/%d done", t.BlocksDone, t.TotalBlocks)
		}

		log.Printf("[Task %s] [%s] 🔌 TCP Sockets: %d open%s | Active Workers: %d/%d\n   └─ Workers: %s",
			p.taskID, p.stage, t.OpenTCPConns, blocksStr, activeCount, len(t.Streams), strings.Join(streamParts, "  "))
	}

	p.lastLog = now
}

func (p *ProgressTracker) Finish(extraInfo string) {
	dur := time.Since(p.startTime)
	log.Printf("[Task %s] [%s] ✅ Part %d/%d finished in %v%s",
		p.taskID, p.stage, p.partIdx, p.totalParts,
		dur.Round(100*time.Millisecond), extraInfo)
}

func generateTaskID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())[:8]
	}
	return hex.EncodeToString(b)
}
