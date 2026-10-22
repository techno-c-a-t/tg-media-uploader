package downloader

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// FileInfo holds metadata about a remote file.
type FileInfo struct {
	URL          string
	Size         int64
	AcceptRanges bool
	Filename     string
}

// ClientOptions allows custom tuning for the downloader client.
type ClientOptions struct {
	Timeout    time.Duration
	UserAgent  string
	MaxRetries int
}

// Client wraps an HTTP client with sensible defaults and high-throughput connection pooling.
type Client struct {
	httpClient *http.Client
	userAgent  string
	maxRetries int
}

const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"

// NewClient creates a new downloader client with default options and the specified timeout.
func NewClient(timeout time.Duration) *Client {
	return NewClientWithOptions(ClientOptions{Timeout: timeout})
}

// NewClientWithOptions creates a new downloader client with customizable options.
func NewClientWithOptions(opts ClientOptions) *Client {
	if opts.Timeout <= 0 {
		opts.Timeout = 45 * time.Minute
	}
	if opts.UserAgent == "" {
		opts.UserAgent = defaultUserAgent
	}
	if opts.MaxRetries <= 0 {
		opts.MaxRetries = 3
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2: false,
		// CRITICAL: Disable HTTP/2 multiplexing so each worker establishes an independent TCP connection!
		// If HTTP/2 is enabled, Go multiplexes all streams over a single TCP connection, causing CDN rate limits to throttle all streams together.
		TLSNextProto:          make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   32, // Essential for multiple parallel streams to the same host!
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}

	return &Client{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   opts.Timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return errors.New("stopped after 10 redirects")
				}
				return nil
			},
		},
		userAgent:  opts.UserAgent,
		maxRetries: opts.MaxRetries,
	}
}

func (c *Client) getUserAgent() string {
	if c.userAgent != "" {
		return c.userAgent
	}
	return defaultUserAgent
}

// Probe inspects the remote URL to check file size and Range support.
func (c *Client) Probe(ctx context.Context, fileURL string) (*FileInfo, error) {
	info := &FileInfo{
		URL: fileURL,
	}

	// 1. Try a HEAD request first
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, fileURL, nil)
	if err == nil {
		req.Header.Set("User-Agent", c.getUserAgent())
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
		req.Header.Set("User-Agent", c.getUserAgent())
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

// DownloadRange downloads a specific byte range in a single stream.
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
	req.Header.Set("User-Agent", c.getUserAgent())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("range request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d for range %s", resp.StatusCode, rangeHeader)
	}

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

	buf := make([]byte, 64*1024)

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

// DownloadRangeParallel downloads a range [start, end] into destPath using multiple concurrent connections.
// If concurrency <= 1 or size < 2MB, it falls back to single-stream DownloadRange.
func (c *Client) DownloadRangeParallel(
	ctx context.Context,
	fileURL string,
	start, end int64,
	destPath string,
	concurrency int,
	onProgress func(written, total int64),
) (string, error) {
	totalSize := end - start + 1
	if totalSize <= 0 {
		return "", errors.New("invalid byte range: total size <= 0")
	}

	// For tiny chunks or concurrency 1, single stream is faster and simpler
	if concurrency <= 1 || totalSize < 2*1024*1024 {
		return c.DownloadRange(ctx, fileURL, start, end, destPath, onProgress)
	}

	if concurrency > 32 {
		concurrency = 32
	}

	// Avoid excessive segmentation on small chunks (min segment: 512 KB)
	if totalSize/int64(concurrency) < 512*1024 {
		concurrency = int(totalSize / (512 * 1024))
		if concurrency < 1 {
			concurrency = 1
		}
	}

	if concurrency <= 1 {
		return c.DownloadRange(ctx, fileURL, start, end, destPath, onProgress)
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return "", fmt.Errorf("failed to create destination dir: %w", err)
	}

	file, err := os.OpenFile(destPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return "", fmt.Errorf("failed to open destination file: %w", err)
	}
	defer file.Close()

	if err := file.Truncate(totalSize); err != nil {
		return "", fmt.Errorf("failed to pre-allocate file: %w", err)
	}

	subSize := (totalSize + int64(concurrency) - 1) / int64(concurrency)
	g, gCtx := errgroup.WithContext(ctx)

	var writtenTotal int64

	for w := 0; w < concurrency; w++ {
		w := w
		wStart := start + int64(w)*subSize
		if wStart > end {
			break
		}
		wEnd := wStart + subSize - 1
		if wEnd > end {
			wEnd = end
		}
		fileOffset := wStart - start

		g.Go(func() error {
			return c.downloadSegmentWithRetry(gCtx, fileURL, wStart, wEnd, file, fileOffset, func(n int64) {
				tot := atomic.AddInt64(&writtenTotal, n)
				if onProgress != nil {
					onProgress(tot, totalSize)
				}
			})
		})
	}

	if err := g.Wait(); err != nil {
		return "", err
	}

	if err := file.Sync(); err != nil {
		return "", fmt.Errorf("failed to sync file: %w", err)
	}

	// Compute MD5 hash via sequential streaming
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", fmt.Errorf("failed to seek destination file: %w", err)
	}

	hasher := md5.New()
	buf := make([]byte, 1024*1024) // 1MB buffer for fast sequential MD5
	if _, err := io.CopyBuffer(hasher, file, buf); err != nil {
		return "", fmt.Errorf("failed to compute file MD5: %w", err)
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (c *Client) downloadSegmentWithRetry(
	ctx context.Context,
	fileURL string,
	start, end int64,
	file *os.File,
	fileOffset int64,
	onBytesWritten func(n int64),
) error {
	maxRetries := c.maxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	var lastErr error

	curStart := start
	curOffset := fileOffset

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create segment request: %w", err)
		}

		rangeHeader := fmt.Sprintf("bytes=%d-%d", curStart, end)
		req.Header.Set("Range", rangeHeader)
		req.Header.Set("User-Agent", c.getUserAgent())

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusPartialContent {
			_ = resp.Body.Close()
			return fmt.Errorf("unexpected status %d for range %s", resp.StatusCode, rangeHeader)
		}

		buf := make([]byte, 256*1024)
		readLoopErr := func() error {
			defer resp.Body.Close()
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				nr, rErr := resp.Body.Read(buf)
				if nr > 0 {
					_, writeErr := file.WriteAt(buf[:nr], curOffset)
					if writeErr != nil {
						return writeErr
					}
					curOffset += int64(nr)
					curStart += int64(nr)
					if onBytesWritten != nil {
						onBytesWritten(int64(nr))
					}
				}

				if rErr != nil {
					if rErr == io.EOF {
						return nil
					}
					return rErr
				}
			}
		}()

		if readLoopErr == nil {
			return nil
		}

		lastErr = readLoopErr
		// If read broke midway, curStart and curOffset have advanced, so next retry continues where it left off!
	}

	return fmt.Errorf("segment bytes=%d-%d failed after %d attempts: %w", start, end, maxRetries, lastErr)
}

func extractFilename(resp *http.Response, fileURL string) string {
	cd := resp.Header.Get("Content-Disposition")
	if cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if filename, ok := params["filename*"]; ok {
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
