package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"tg_file_proxy/config"
	"tg_file_proxy/pkg/pipeline"
	"tg_file_proxy/pkg/telegram"
)

type DownloadRequest struct {
	URL string `json:"url"`
}

func main() {
	urlFlag := flag.String("url", "", "URL of the file to download and push to Telegram")
	dryRunFlag := flag.Bool("dry-run", false, "Simulate Telegram upload locally (saves to ./simulated_tg_channel)")
	chunkMBFlag := flag.Int64("chunk-mb", 0, "Override chunk size in MB (e.g. 5 or 10 for testing)")
	flag.Parse()

	cfg := config.LoadFromEnv()
	if *chunkMBFlag > 0 {
		cfg.ChunkSizeMB = *chunkMBFlag
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runServer := func(runCtx context.Context, uploader telegram.Uploader) error {
		orch := pipeline.NewOrchestrator(cfg, uploader)

		targetURL := *urlFlag
		if targetURL != "" {
			log.Printf("Starting immediate download task for: %s (Chunk size: %d MB)", targetURL, cfg.ChunkSizeMB)
			_, err := orch.ProcessURL(runCtx, targetURL)
			return err
		}

		mux := http.NewServeMux()
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK\n"))
		})

		mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(w, "Method not allowed. Use POST.", http.StatusMethodNotAllowed)
				return
			}

			var req DownloadRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
				http.Error(w, "Invalid JSON. Expected {\"url\": \"https://...\"}", http.StatusBadRequest)
				return
			}

			go func(downloadURL string) {
				taskCtx, taskCancel := context.WithCancel(context.Background())
				defer taskCancel()
				log.Printf("[HTTP] New task received for: %s", downloadURL)
				if _, err := orch.ProcessURL(taskCtx, downloadURL); err != nil {
					log.Printf("[HTTP] Task failed: %v", err)
				}
			}(req.URL)

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status":  "task_accepted",
				"message": fmt.Sprintf("Download task started for %s", req.URL),
			})
		})

		server := &http.Server{
			Addr:    ":" + cfg.HTTPPort,
			Handler: mux,
		}

		go func() {
			<-runCtx.Done()
			_ = server.Shutdown(context.Background())
		}()

		log.Printf("🚀 tg_file_proxy server listening on port :%s", cfg.HTTPPort)
		log.Println("Send a download task via:")
		log.Printf("  curl -X POST http://localhost:%s/download -d '{\"url\":\"https://...\"}' -H 'Content-Type: application/json'", cfg.HTTPPort)

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	}

	if *dryRunFlag || (cfg.AppID == 0 && *urlFlag != "") {
		log.Println("⚠️ Running in DRY-RUN mode (Simulated Telegram channel at ./simulated_tg_channel)")
		mockUploader := telegram.NewMockUploader("./simulated_tg_channel")
		if err := runServer(ctx, mockUploader); err != nil && err != context.Canceled {
			log.Fatalf("Fatal error: %v", err)
		}
		return
	}

	if cfg.AppID == 0 || cfg.AppHash == "" {
		log.Println("WARNING: TG_APP_ID or TG_APP_HASH is not set. Please provide them via environment variables.")
	}

	tgClient, err := telegram.NewClient(telegram.Config{
		AppID:       cfg.AppID,
		AppHash:     cfg.AppHash,
		BotToken:    cfg.BotToken,
		TargetPeer:  cfg.TargetPeer,
		SessionFile: "tg_session.json",
	})
	if err != nil {
		log.Fatalf("Failed to initialize Telegram client: %v", err)
	}

	tgClient.SetURLHandler(func(handlerCtx context.Context, targetURL string, reply func(text string)) {
		log.Printf("[Bot Listener] Received download request via Telegram: %s", targetURL)
		reply(fmt.Sprintf("🚀 Задача принята!\nФайл: %s\nНачинаю скачивание на VPS и выгрузку в канал...", targetURL))

		go func() {
			taskCtx, taskCancel := context.WithCancel(context.Background())
			defer taskCancel()

			orch := pipeline.NewOrchestrator(cfg, tgClient)
			m, err := orch.ProcessURL(taskCtx, targetURL)
			if err != nil {
				log.Printf("[Bot Listener] Task error: %v", err)
				reply(fmt.Sprintf("❌ Ошибка: %v", err))
				return
			}
			reply(fmt.Sprintf("🎉 Готово! Файл '%s' (%.2f MB, %d частей) успешно загружен в канал!",
				m.Filename, float64(m.TotalSize)/(1024*1024), len(m.Parts)))
		}()
	})

	err = tgClient.Start(ctx, func(clientCtx context.Context) error {
		return runServer(clientCtx, tgClient)
	})

	if err != nil && err != context.Canceled {
		log.Fatalf("Fatal error: %v", err)
	}
}
