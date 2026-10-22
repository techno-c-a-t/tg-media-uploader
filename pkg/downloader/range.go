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

const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"

// Probe inspects the remote URL to check file size and Range support.
func (c *Client) Probe(ctx context.Context, fileURL string) (*FileInfo, error) {
	info := &FileInfo{
		URL: fileURL,
	}

	// 1. Try a HEAD request first
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fileURL, nil)
	if err == nil {
		req.Header.Set("User-Agent", defaultUserAgent)
		resp, err := c.httpClient.Do(req)
		if err == nil {
			info.Filename = extractFilename(resp, fileURL)
			if strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes") {
				info.AcceptRanges = true
			}
			if resp.ContentLength > 0 {
				info.Size = resp.ContentLength
			}
			_ = resp.Body.Close()
		}
	}

	// 2. If size is 0 or Range unconfirmed, execute GET with Range: bytes=0-0
	if info.Size <= 0 || !info.AcceptRanges {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create range probe: %w", err)
		}
		req.Header.Set("User-Agent", defaultUserAgent)
		req.Header.Set("Range", "bytes=0-0")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to execute range probe: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusPartialContent || strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes") {
			info.AcceptRanges = true
		}

		if info.Filename == "" {
			info.Filename = extractFilename(resp, fileURL)
		}

		cr := resp.Header.Get("Content-Range")
		if cr != "" {
			parts := strings.Split(cr, "/")
			if len(parts) == 2 {
				if totalSize, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil && totalSize > 0 {
					info.Size = totalSize
				}
			}
		}

		if info.Size <= 0 && resp.ContentLength > 0 {
			info.Size = resp.ContentLength
		}
	}

	// 3. Fallback: check URL query parameters (e.g. ?full_size=... or ?size=...)
	if info.Size <= 0 {
		if parsed, err := url.Parse(fileURL); err == nil {
			q := parsed.Query()
			for _, param := range []string{"full_size", "size", "filesize"} {
				if val := q.Get(param); val != "" {
					if s, err := strconv.ParseInt(val, 10, 64); err == nil && s > 0 {
						info.Size = s
						break
					}
				}
			}
		}
	}

	if info.Filename == "" {
		info.Filename = fmt.Sprintf("file_%d.bin", time.Now().Unix())
	}

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
	req.Header.Set("User-Agent", defaultUserAgent)

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
