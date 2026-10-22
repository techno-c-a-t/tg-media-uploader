package downloader_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"tg_file_proxy/pkg/downloader"
)

func TestParallelDownload(t *testing.T) {
	// 1. Generate 12 MB of pseudo-random test data
	testDataSize := int64(12 * 1024 * 1024)
	testData := make([]byte, testDataSize)
	_, err := rand.Read(testData)
	if err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}

	expectedMD5Raw := md5.Sum(testData)
	expectedMD5 := hex.EncodeToString(expectedMD5Raw[:])

	// 2. Set up mock HTTP server supporting HTTP Range requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Disposition", `attachment; filename="benchmark_test.bin"`)

		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", strconv.FormatInt(testDataSize, 10))
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(testData)
			return
		}

		// Parse Range: bytes=start-end
		if !strings.HasPrefix(rangeHdr, "bytes=") {
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		parts := strings.Split(strings.TrimPrefix(rangeHdr, "bytes="), "-")
		if len(parts) != 2 {
			http.Error(w, "invalid range format", http.StatusBadRequest)
			return
		}

		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || end >= testDataSize {
			end = testDataSize - 1
		}

		if start > end || start >= testDataSize {
			http.Error(w, "range unsatisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}

		length := end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, testDataSize))
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.WriteHeader(http.StatusPartialContent)

		if r.Method != http.MethodHead {
			_, _ = w.Write(testData[start : end+1])
		}
	}))
	defer server.Close()

	client := downloader.NewClient(10 * time.Second)

	// Test Probe
	info, err := client.Probe(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("Probe failed: %v", err)
	}
	if info.Size != testDataSize {
		t.Fatalf("Expected size %d, got %d", testDataSize, info.Size)
	}
	if !info.AcceptRanges {
		t.Fatalf("Expected AcceptRanges to be true")
	}
	if info.Filename != "benchmark_test.bin" {
		t.Fatalf("Expected filename benchmark_test.bin, got %s", info.Filename)
	}

	// Test Parallel Download with 8 workers
	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "downloaded_parallel.bin")

	var progressCalls int64
	hash, err := client.DownloadRangeParallel(
		context.Background(),
		server.URL,
		0,
		testDataSize-1,
		outPath,
		8,
		func(written, total int64) {
			progressCalls++
		},
	)
	if err != nil {
		t.Fatalf("DownloadRangeParallel failed: %v", err)
	}

	if hash != expectedMD5 {
		t.Fatalf("MD5 mismatch! Expected %s, got %s", expectedMD5, hash)
	}

	// Verify file size and binary content on disk
	downloadedBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}
	if int64(len(downloadedBytes)) != testDataSize {
		t.Fatalf("Size on disk mismatch: expected %d, got %d", testDataSize, len(downloadedBytes))
	}
	if !bytes.Equal(downloadedBytes, testData) {
		t.Fatalf("Downloaded file binary content differs from original")
	}

	t.Logf("✅ Successfully downloaded 12 MB with 8 workers, MD5 matches (%s), progress callbacks: %d", hash, progressCalls)
}
