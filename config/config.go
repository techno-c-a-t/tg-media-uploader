package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// Config represents all service settings loaded from environment or .env file.
type Config struct {
	// Telegram credentials & destination
	AppID       int
	AppHash     string
	BotToken    string
	Phone       string
	TargetPeer  string
	SessionFile string

	// Chunk & Storage
	ChunkSizeMB int64
	TempDir     string

	// Download engine parameters
	DownloadConcurrency int
	DownloadTimeoutMin  int
	DownloadMaxRetries  int
	DownloadUserAgent   string

	// Upload engine parameters
	UploadPartSizeKB int
	UploadThreads    int

	// Server networking
	HTTPPort string
	HostPort string
}

// LoadFromEnv loads configuration from environment variables and parses local .env if present.
func LoadFromEnv() *Config {
	loadDotEnv(".env")

	// Support both TG_APP_ID and API_ID
	appIDStr := getEnv("TG_APP_ID", "")
	if appIDStr == "" {
		appIDStr = getEnv("API_ID", "0")
	}
	appID, _ := strconv.Atoi(strings.TrimSpace(appIDStr))

	// Support both TG_APP_HASH and API_HASH
	appHash := getEnv("TG_APP_HASH", "")
	if appHash == "" {
		appHash = getEnv("API_HASH", "")
	}
	appHash = strings.TrimSpace(appHash)

	chunkSizeMB, _ := strconv.ParseInt(getEnv("CHUNK_SIZE_MB", "500"), 10, 64)
	if chunkSizeMB <= 0 {
		chunkSizeMB = 500
	}

	downloadConcurrency, _ := strconv.Atoi(getEnv("CONCURRENT_DOWNLOAD_STREAMS", "8"))
	if downloadConcurrency <= 0 {
		downloadConcurrency = 8
	}
	if downloadConcurrency > 32 {
		downloadConcurrency = 32
	}

	downloadTimeoutMin, _ := strconv.Atoi(getEnv("DOWNLOAD_TIMEOUT_MINUTES", "45"))
	if downloadTimeoutMin <= 0 {
		downloadTimeoutMin = 45
	}

	downloadMaxRetries, _ := strconv.Atoi(getEnv("DOWNLOAD_MAX_RETRIES", "3"))
	if downloadMaxRetries < 1 {
		downloadMaxRetries = 1
	}

	downloadUserAgent := getEnv("DOWNLOAD_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")

	uploadPartSizeKB, _ := strconv.Atoi(getEnv("TG_UPLOAD_PART_SIZE_KB", "512"))
	if uploadPartSizeKB <= 0 {
		uploadPartSizeKB = 512
	}

	uploadThreads, _ := strconv.Atoi(getEnv("TG_UPLOAD_THREADS", "4"))
	if uploadThreads <= 0 {
		uploadThreads = 4
	}

	tempDir := getEnv("TEMP_DIR", "/tmp/tg_file_proxy")
	_ = os.MkdirAll(tempDir, 0755)

	sessionFile := getEnv("TG_SESSION_FILE", "")
	if sessionFile == "" {
		if _, err := os.Stat("/app/data"); err == nil {
			sessionFile = "/app/data/tg_session.json"
		} else {
			sessionFile = "tg_session.json"
		}
	}

	return &Config{
		AppID:               appID,
		AppHash:             appHash,
		BotToken:            strings.TrimSpace(getEnv("TG_BOT_TOKEN", "")),
		Phone:               strings.TrimSpace(getEnv("TG_PHONE", "")),
		TargetPeer:          strings.TrimSpace(getEnv("TG_PEER", "self")),
		SessionFile:         sessionFile,
		ChunkSizeMB:         chunkSizeMB,
		TempDir:             tempDir,
		DownloadConcurrency: downloadConcurrency,
		DownloadTimeoutMin:  downloadTimeoutMin,
		DownloadMaxRetries:  downloadMaxRetries,
		DownloadUserAgent:   downloadUserAgent,
		UploadPartSizeKB:    uploadPartSizeKB,
		UploadThreads:       uploadThreads,
		HTTPPort:            getEnv("HTTP_PORT", "8080"),
		HostPort:            getEnv("HOST_PORT", "8088"),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func loadDotEnv(filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			k := strings.TrimSpace(parts[0])
			v := strings.TrimSpace(parts[1])
			v = strings.Trim(v, `"'`)
			if os.Getenv(k) == "" {
				_ = os.Setenv(k, v)
			}
		}
	}
}
