package assembler_test

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"tg_file_proxy/pkg/assembler"
	"tg_file_proxy/pkg/manifest"
)

func TestAssembleFromDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tg_proxy_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	totalSize := int64(5 * 1024 * 1024) // 5 MB
	chunkSize := int64(2 * 1024 * 1024) // 2 MB chunks (3 chunks total)

	originalData := make([]byte, totalSize)
	if _, err := rand.Read(originalData); err != nil {
		t.Fatal(err)
	}

	origHasher := md5.New()
	origHasher.Write(originalData)
	origMD5 := hex.EncodeToString(origHasher.Sum(nil))

	filename := "test_large_file.bin"
	m := &manifest.Manifest{
		TaskID:     "test1234",
		Filename:   filename,
		TotalSize:  totalSize,
		ChunkSize:  chunkSize,
		TotalParts: 3,
		FileMD5:    origMD5,
		Parts:      make([]manifest.PartInfo, 0, 3),
	}

	// Create chunks
	for i := 0; i < 3; i++ {
		start := int64(i) * chunkSize
		end := start + chunkSize
		if end > totalSize {
			end = totalSize
		}

		partData := originalData[start:end]
		partHasher := md5.New()
		partHasher.Write(partData)
		partMD5 := hex.EncodeToString(partHasher.Sum(nil))

		chunkPath := filepath.Join(tmpDir, filename+manifestFormat(i))
		if err := os.WriteFile(chunkPath, partData, 0644); err != nil {
			t.Fatal(err)
		}

		m.Parts = append(m.Parts, manifest.PartInfo{
			Index:     i,
			StartByte: start,
			EndByte:   end - 1,
			Size:      int64(len(partData)),
			MD5:       partMD5,
		})
	}

	outputPath := filepath.Join(tmpDir, "assembled_output.bin")
	err = assembler.AssembleFromDirectory(m, tmpDir, outputPath, nil)
	if err != nil {
		t.Fatalf("Assemble failed: %v", err)
	}

	assembledData, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}

	if int64(len(assembledData)) != totalSize {
		t.Fatalf("Size mismatch: expected %d, got %d", totalSize, len(assembledData))
	}

	assembledHasher := md5.New()
	assembledHasher.Write(assembledData)
	assembledMD5 := hex.EncodeToString(assembledHasher.Sum(nil))

	if assembledMD5 != origMD5 {
		t.Fatalf("MD5 mismatch: expected %s, got %s", origMD5, assembledMD5)
	}

	t.Logf("Assembly test PASSED: MD5 match %s", assembledMD5)
}

func manifestFormat(idx int) string {
	return filepath.Ext(filepath.Join("", "dummy")) + ".part_" + pad(idx)
}

func pad(n int) string {
	res := string(rune('0'+n/1000%10)) +
		string(rune('0'+n/100%10)) +
		string(rune('0'+n/10%10)) +
		string(rune('0'+n%10))
	return res
}
