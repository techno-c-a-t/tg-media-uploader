package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"tg_file_proxy/pkg/assembler"
	"tg_file_proxy/pkg/manifest"
)

func main() {
	manifestPath := flag.String("manifest", "manifest.json", "Path to manifest.json file")
	chunksDir := flag.String("chunks", ".", "Directory containing the downloaded chunk files")
	outputPath := flag.String("out", "", "Output path for the reassembled file (optional, defaults to original filename)")
	flag.Parse()

	if *manifestPath == "" {
		log.Fatal("Error: please specify -manifest path")
	}

	m, err := manifest.LoadFromFile(*manifestPath)
	if err != nil {
		log.Fatalf("Failed to read manifest file %s: %v", *manifestPath, err)
	}

	targetOut := *outputPath
	if targetOut == "" {
		targetOut = filepath.Join(*chunksDir, m.Filename)
	}

	log.Println("==================================================")
	log.Printf("🔨 Telegram Chunk Assembler")
	log.Printf("Task ID:     %s", m.TaskID)
	log.Printf("Target File: %s", targetOut)
	log.Printf("Total Size:  %.2f MB (%d bytes)", float64(m.TotalSize)/(1024*1024), m.TotalSize)
	log.Printf("Total Parts: %d", len(m.Parts))
	log.Println("==================================================")

	startTime := time.Now()

	err = assembler.AssembleFromDirectory(m, *chunksDir, targetOut, func(chunkIdx, totalChunks int, written, total int64) {
		pct := float64(written) / float64(total) * 100.0
		fmt.Printf("\r⏳ Stitched part %d/%d | %.2f MB / %.2f MB (%.1f%%)",
			chunkIdx, totalChunks, float64(written)/(1024*1024), float64(total)/(1024*1024), pct)
	})
	fmt.Println()

	if err != nil {
		log.Fatalf("\n❌ Assembly failed: %v", err)
	}

	duration := time.Since(startTime)
	speedMBs := (float64(m.TotalSize) / (1024 * 1024)) / duration.Seconds()

	log.Printf("✅ Assembly successfully finished in %v (%.2f MB/s)!", duration.Round(time.Millisecond), speedMBs)
	log.Printf("📁 Output file ready at: %s", targetOut)
	os.Exit(0)
}
