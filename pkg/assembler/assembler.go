package assembler

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"tg_file_proxy/pkg/manifest"
)

// AssembleFromDirectory combines chunks in chunkDir according to m and writes the output file.
func AssembleFromDirectory(
	m *manifest.Manifest,
	chunkDir string,
	outputPath string,
	onProgress func(chunkIndex, totalChunks int, writtenBytes, totalBytes int64),
) error {
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create destination dir: %w", err)
	}

	outFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open output file: %w", err)
	}
	defer outFile.Close()

	overallHasher := md5.New()
	var totalWritten int64

	buf := make([]byte, 1024*1024) // 1 MB buffer for fast sequential disk I/O on local PC

	for i, part := range m.Parts {
		chunkFileName := fmt.Sprintf("%s.part_%04d", m.Filename, part.Index)
		chunkFilePath := filepath.Join(chunkDir, chunkFileName)

		// Check if file exists, if not check alternative naming
		if _, err := os.Stat(chunkFilePath); os.IsNotExist(err) {
			altName := fmt.Sprintf("part_%d.bin", part.Index)
			altPath := filepath.Join(chunkDir, altName)
			if _, err := os.Stat(altPath); err == nil {
				chunkFilePath = altPath
			} else {
				return fmt.Errorf("chunk file not found for part %d (searched %s and %s)", part.Index, chunkFileName, altName)
			}
		}

		// Verify chunk integrity before appending
		partHash, err := hashFileMD5(chunkFilePath)
		if err != nil {
			return fmt.Errorf("failed to calculate MD5 for part %d: %w", part.Index, err)
		}
		if part.MD5 != "" && partHash != part.MD5 {
			return fmt.Errorf("checksum mismatch on part %d: expected %s, got %s", part.Index, part.MD5, partHash)
		}

		// Stream chunk into output file
		chunkFile, err := os.Open(chunkFilePath)
		if err != nil {
			return fmt.Errorf("failed to open chunk %s: %w", chunkFilePath, err)
		}

		for {
			nr, rErr := chunkFile.Read(buf)
			if nr > 0 {
				nw, wErr := outFile.Write(buf[:nr])
				if wErr != nil {
					chunkFile.Close()
					return fmt.Errorf("error writing output file: %w", wErr)
				}
				overallHasher.Write(buf[:nr])
				totalWritten += int64(nw)
				if onProgress != nil {
					onProgress(i+1, len(m.Parts), totalWritten, m.TotalSize)
				}
			}
			if rErr != nil {
				if rErr == io.EOF {
					break
				}
				chunkFile.Close()
				return fmt.Errorf("error reading chunk: %w", rErr)
			}
		}
		chunkFile.Close()
	}

	finalMD5 := hex.EncodeToString(overallHasher.Sum(nil))
	if m.FileMD5 != "" && m.FileMD5 != finalMD5 {
		return fmt.Errorf("final file MD5 mismatch: expected %s, got %s", m.FileMD5, finalMD5)
	}

	return nil
}

func hashFileMD5(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := md5.New()
	buf := make([]byte, 512*1024)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
