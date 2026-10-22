package downloader

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// FileInfo holds metadata about a remote file.
type FileInfo struct {
	URL          string
	Size         int64
	AcceptRanges bool
	Filename     string
}

// Client wraps an HTTP client with sensible defaults.
type Client struct {
	httpClient *http.Client
}

// NewClient creates a new downloader client.
func NewClient(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return errors.New("stopped after 10 redirects")
				}
				return nil
			},
		},
	}
}

// Probe inspects the remote URL to check file size and Range support.
func (c *Client) Probe(ctx context.Context, fileURL string) (*FileInfo, error) {
	// First, try a HEAD request
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fileURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HEAD request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; TgFileProxy/1.0)")

	resp, err := c.httpClient.Do(req)
	// If HEAD is not supported (405, 403, 501), fallback to GET with Range: bytes=0-0
	if err != nil || resp.StatusCode >= 400 {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}

		req, err = http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create GET range probe: %w", err)
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; TgFileProxy/1.0)")
		req.Header.Set("Range", "bytes=0-0")

		resp, err = c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to execute probe request: %w", err)
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("server responded with status %d", resp.StatusCode)
	}

	info := &FileInfo{
		URL: fileURL,
	}

	// 1. Determine Range support
	acceptRanges := resp.Header.Get("Accept-Ranges")
	contentRange := resp.Header.Get("Content-Range")
	if strings.EqualFold(acceptRanges, "bytes") || resp.StatusCode == http.StatusPartialContent || contentRange != "" {
		info.AcceptRanges = true
	}

	// 2. Determine file size
	if contentRange != "" {
		// Content-Range: bytes 0-0/12345678
		parts := strings.Split(contentRange, "/")
		if len(parts) == 2 {
			if totalSize, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil {
				info.Size = totalSize
			}
		}
	}
	if info.Size == 0 && resp.ContentLength > 0 {
		info.Size = resp.ContentLength
	}

	// 3. Extract filename
	info.Filename = extractFilename(resp, fileURL)

	return info, nil
}

// DownloadRange downloads a specific byte range and streams it directly to destPath while computing MD5.
func (c *Client) DownloadRange(
	ctx context.Context,
	fileURL string,
	start, end int64,
	destPath string,
	onProgress func(written, total int64),
) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create range request: %w", err)
	}

	rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)
	req.Header.Set("Range", rangeHeader)
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; TgFileProxy/1.0)")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("range request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d for range %s", resp.StatusCode, rangeHeader)
	}

	// Ensure destination directory exists
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return "", fmt.Errorf("failed to create destination dir: %w", err)
	}

	file, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to open destination file: %w", err)
	}
	defer file.Close()

	hasher := md5.New()
	targetSize := end - start + 1
	var written int64

	buf := make([]byte, 64*1024) // 64 KB buffer (extremely light on RAM)

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		nr, readErr := resp.Body.Read(buf)
		if nr > 0 {
			nw, writeErr := file.Write(buf[0:nr])
			if writeErr != nil {
				return "", fmt.Errorf("error writing to file: %w", writeErr)
			}
			hasher.Write(buf[0:nr])
			written += int64(nw)
			if onProgress != nil {
				onProgress(written, targetSize)
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return "", fmt.Errorf("error reading response body: %w", readErr)
		}
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func extractFilename(resp *http.Response, fileURL string) string {
	cd := resp.Header.Get("Content-Disposition")
	if cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if filename, ok := params["filename*"]; ok {
				// Handle UTF-8''encoded names if present
				parts := strings.SplitN(filename, "''", 2)
				if len(parts) == 2 {
					if decoded, err := url.PathUnescape(parts[1]); err == nil {
						return filepath.Base(decoded)
					}
				}
			}
			if filename, ok := params["filename"]; ok {
				return filepath.Base(filename)
			}
		}
	}

	parsed, err := url.Parse(fileURL)
	if err == nil {
		base := path.Base(parsed.Path)
		if base != "" && base != "." && base != "/" {
			return base
		}
	}

	return fmt.Sprintf("file_%d.bin", time.Now().Unix())
}
