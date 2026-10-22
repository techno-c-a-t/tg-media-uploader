package downloader

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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

// StreamStatus represents the lifecycle state of a parallel download worker.
type StreamStatus string

const (
	StatusConnecting   StreamStatus = "CONNECT"
	StatusTLSHandshake StreamStatus = "TLS"
	StatusStreaming    StreamStatus = "STREAM"
	StatusRetrying     StreamStatus = "RETRY"
	StatusDone         StreamStatus = "DONE"
	StatusError        StreamStatus = "ERR"
)

// StreamProgress represents telemetry for an individual parallel worker in the pool.
type StreamProgress struct {
	ID           int
	CurrentBlock int
	Start        int64
	End          int64
	Written      int64
	Total        int64
	BlocksDone   int
	Status       StreamStatus
	ReusedTCP    bool
}

// DownloadTelemetry provides real-time metrics across all parallel workers and dynamic blocks.
type DownloadTelemetry struct {
	TotalWritten int64
	TotalSize    int64
	OpenTCPConns int64
	TotalBlocks  int
	BlocksDone   int
	Streams      []StreamProgress
}

// DownloadProgressFunc is invoked periodically with progress and stream-level telemetry.
type DownloadProgressFunc func(written, total int64, telemetry *DownloadTelemetry)

// Client wraps an HTTP client with high-throughput connection pooling and socket tuning.
type Client struct {
	httpClient   *http.Client
	userAgent    string
	maxRetries   int
	openTCPConns int64
}

const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"

// NewClient creates a new downloader client with default options and the specified timeout.
func NewClient(timeout time.Duration) *Client {
	return NewClientWithOptions(ClientOptions{Timeout: timeout})
}

// NewClientWithOptions creates a new downloader client with customizable options and FDM-style socket tuning.
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
			Control: func(network, address string, c syscall.RawConn) error {
				return c.Control(func(fd uintptr) {
					// Free Download Manager (FDM) optimization:
					// Expand socket receive buffer (SO_RCVBUF) to 512 KB to avoid kernel packet drops on high-bandwidth WAN connections
					_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 512*1024)
				})
			},
		}).DialContext,
		ForceAttemptHTTP2: false,
		// CRITICAL: Disable HTTP/2 multiplexing so each worker opens an independent TCP socket.
		// If HTTP/2 is enabled, Go multiplexes all streams over a single TCP socket, causing CDN rate limits to throttle all streams together.
		TLSNextProto:          make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   32,
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
	onProgress DownloadProgressFunc,
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

	buf := make([]byte, 256*1024)

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
				onProgress(written, targetSize, &DownloadTelemetry{
					TotalWritten: written,
					TotalSize:    targetSize,
					OpenTCPConns: 1,
					TotalBlocks:  1,
					BlocksDone:   0,
					Streams: []StreamProgress{
						{ID: 1, Start: start, End: end, Written: written, Total: targetSize, Status: StatusStreaming},
					},
				})
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

type blockTask struct {
	id    int
	start int64
	end   int64
}

// DownloadRangeParallel downloads a range [start, end] into destPath using a dynamic FDM-style work queue.
// Instead of static partitions, blocks (8-16 MB) are pushed to a concurrent queue so fast workers automatically steal work.
func (c *Client) DownloadRangeParallel(
	ctx context.Context,
	fileURL string,
	start, end int64,
	destPath string,
	concurrency int,
	onProgress DownloadProgressFunc,
) (string, error) {
	totalSize := end - start + 1
	if totalSize <= 0 {
		return "", errors.New("invalid byte range: total size <= 0")
	}

	if concurrency <= 1 || totalSize < 2*1024*1024 {
		return c.DownloadRange(ctx, fileURL, start, end, destPath, onProgress)
	}

	if concurrency > 32 {
		concurrency = 32
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

	// -------------------------------------------------------------------------
	// FDM DYNAMIC WORK QUEUE:
	// Divide total chunk range into dynamic 8 MB blocks.
	// -------------------------------------------------------------------------
	blockSize := int64(8 * 1024 * 1024) // 8 MB dynamic block
	if totalSize/int64(concurrency) < blockSize {
		blockSize = (totalSize + int64(concurrency*2) - 1) / int64(concurrency * 2)
	}
	if blockSize < 1024*1024 {
		blockSize = 1024 * 1024 // min 1 MB
	}

	var tasks []blockTask
	taskID := 1
	for bStart := start; bStart <= end; bStart += blockSize {
		bEnd := bStart + blockSize - 1
		if bEnd > end {
			bEnd = end
		}
		tasks = append(tasks, blockTask{
			id:    taskID,
			start: bStart,
			end:   bEnd,
		})
		taskID++
	}
	totalBlocks := len(tasks)

	taskCh := make(chan blockTask, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	g, gCtx := errgroup.WithContext(ctx)
	var writtenTotal int64
	var completedBlocks int64

	workers := make([]*StreamProgress, concurrency)
	var workerMu sync.Mutex

	for w := 0; w < concurrency; w++ {
		sp := &StreamProgress{
			ID:     w + 1,
			Status: StatusConnecting,
		}
		workers[w] = sp

		wIdx := w
		g.Go(func() error {
			worker := workers[wIdx]

			for task := range taskCh {
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				default:
				}

				workerMu.Lock()
				worker.CurrentBlock = task.id
				worker.Start = task.start
				worker.End = task.end
				worker.Total = task.end - task.start + 1
				worker.Written = 0
				worker.Status = StatusConnecting
				workerMu.Unlock()

				fileOffset := task.start - start
				err := c.downloadBlockWithRetry(gCtx, fileURL, task.start, task.end, file, fileOffset, worker, &workerMu, func(n int64) {
					tot := atomic.AddInt64(&writtenTotal, n)
					if onProgress != nil {
						workerMu.Lock()
						snapshot := make([]StreamProgress, len(workers))
						for i, s := range workers {
							if s != nil {
								snapshot[i] = *s
								snapshot[i].Written = atomic.LoadInt64(&s.Written)
							}
						}
						workerMu.Unlock()

						onProgress(tot, totalSize, &DownloadTelemetry{
							TotalWritten: tot,
							TotalSize:    totalSize,
							OpenTCPConns: atomic.LoadInt64(&c.openTCPConns),
							TotalBlocks:  totalBlocks,
							BlocksDone:   int(atomic.LoadInt64(&completedBlocks)),
							Streams:      snapshot,
						})
					}
				})
				if err != nil {
					return err
				}

				workerMu.Lock()
				worker.BlocksDone++
				workerMu.Unlock()
				atomic.AddInt64(&completedBlocks, 1)
			}

			workerMu.Lock()
			worker.Status = StatusDone
			workerMu.Unlock()
			return nil
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

func (c *Client) downloadBlockWithRetry(
	ctx context.Context,
	fileURL string,
	start, end int64,
	file *os.File,
	fileOffset int64,
	worker *StreamProgress,
	workerMu *sync.Mutex,
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
			workerMu.Lock()
			worker.Status = StatusRetrying
			workerMu.Unlock()
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create segment request: %w", err)
		}

		rangeHeader := fmt.Sprintf("bytes=%d-%d", curStart, end)
		req.Header.Set("Range", rangeHeader)
		req.Header.Set("User-Agent", c.getUserAgent())

		// Real-time TCP socket and connection lifecycle tracing
		trace := &httptrace.ClientTrace{
			ConnectStart: func(network, addr string) {
				workerMu.Lock()
				worker.Status = StatusConnecting
				workerMu.Unlock()
			},
			TLSHandshakeStart: func() {
				workerMu.Lock()
				worker.Status = StatusTLSHandshake
				workerMu.Unlock()
			},
			GotConn: func(info httptrace.GotConnInfo) {
				atomic.AddInt64(&c.openTCPConns, 1)
				workerMu.Lock()
				worker.ReusedTCP = info.Reused
				worker.Status = StatusStreaming
				workerMu.Unlock()
			},
		}
		req = req.WithContext(httptrace.WithClientTrace(ctx, trace))

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			workerMu.Lock()
			worker.Status = StatusRetrying
			workerMu.Unlock()
			log.Printf("[Worker #%d Block #%d] ⚠️ Attempt %d/%d network error: %v. Retrying...", worker.ID, worker.CurrentBlock, attempt+1, maxRetries, err)
			continue
		}

		if resp.StatusCode != http.StatusPartialContent {
			_ = resp.Body.Close()
			atomic.AddInt64(&c.openTCPConns, -1)
			workerMu.Lock()
			worker.Status = StatusError
			workerMu.Unlock()
			if resp.StatusCode == http.StatusForbidden {
				return fmt.Errorf("HTTP 403 Forbidden for range %s (remote URL signature/token has expired!)", rangeHeader)
			}
			return fmt.Errorf("unexpected status %d for range %s", resp.StatusCode, rangeHeader)
		}

		buf := make([]byte, 256*1024)
		readLoopErr := func() error {
			defer func() {
				_ = resp.Body.Close()
				atomic.AddInt64(&c.openTCPConns, -1)
			}()

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
					atomic.AddInt64(&worker.Written, int64(nr))
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
		log.Printf("[Worker #%d Block #%d] ⚠️ Read interrupted at byte %d (%v). Resuming next attempt from byte %d...",
			worker.ID, worker.CurrentBlock, curStart, readLoopErr, curStart)
	}

	workerMu.Lock()
	worker.Status = StatusError
	workerMu.Unlock()

	return fmt.Errorf("block %d bytes=%d-%d failed after %d attempts: %w", worker.CurrentBlock, start, end, maxRetries, lastErr)
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
