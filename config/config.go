package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// Config represents service settings.
type Config struct {
	AppID       int
	AppHash     string
	BotToken    string
	Phone       string
	TargetPeer  string
	ChunkSizeMB int64
	TempDir     string
	HTTPPort    string
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

	tempDir := getEnv("TEMP_DIR", "/tmp/tg_file_proxy")
	_ = os.MkdirAll(tempDir, 0755)

	return &Config{
		AppID:       appID,
		AppHash:     appHash,
		BotToken:    strings.TrimSpace(getEnv("TG_BOT_TOKEN", "")),
		Phone:       strings.TrimSpace(getEnv("TG_PHONE", "")),
		TargetPeer:  strings.TrimSpace(getEnv("TG_PEER", "self")),
		ChunkSizeMB: chunkSizeMB,
		TempDir:     tempDir,
		HTTPPort:    getEnv("HTTP_PORT", "8080"),
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
			// remove quotes if present
			v = strings.Trim(v, `"'`)
			if os.Getenv(k) == "" {
				_ = os.Setenv(k, v)
			}
		}
	}
}
