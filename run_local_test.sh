#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=========================================================="
echo "🧪 STARTING LOCAL END-TO-END INTEGRATION TEST"
echo "=========================================================="

# 1. Clean up old test data
rm -rf ./simulated_tg_channel /tmp/tg_file_proxy ./restored_sample.iso
mkdir -p /tmp/tg_file_proxy

# 2. Start Mock HTTP Server with 25MB file
echo "1. Starting Mock HTTP Range Server on port 9099..."
./mock_http -port 9099 -size 25 > /tmp/mock_http.log 2>&1 &
MOCK_PID=$!

trap "kill $MOCK_PID 2>/dev/null || true" EXIT
sleep 1

# Check server is up
curl -s http://127.0.0.1:9099/info | jq . || curl -s http://127.0.0.1:9099/info

# 3. Run tg_proxy in dry-run mode with 10MB chunk size
echo ""
echo "2. Running tg_proxy (URL -> Range Chunks -> Simulated TG Channel -> Disk Cleanup)..."
./tg_proxy -dry-run -chunk-mb 10 -url "http://127.0.0.1:9099/large_sample.iso"

# Verify VPS temp directory has NO leftover chunks
REMAINING_CHUNKS=$(find /tmp/tg_file_proxy -name "*.tmp" | wc -l)
echo "🔍 Checking disk cleanup: Leftover .tmp chunks in /tmp/tg_file_proxy = $REMAINING_CHUNKS"
if [ "$REMAINING_CHUNKS" -ne 0 ]; then
    echo "❌ Error: Temporary chunk files were not cleaned up!"
    exit 1
fi
echo "✅ Disk cleanup verified: 0 bytes leaked on temporary disk."

# Find generated manifest
MANIFEST_FILE=$(find /tmp/tg_file_proxy -name "*_manifest.json" | head -n 1)
echo "📄 Found manifest: $MANIFEST_FILE"

# 4. Run Assembler to reconstruct file
echo ""
echo "3. Running tg-assembler to reconstruct file..."
./tg-assembler -manifest "$MANIFEST_FILE" -chunks ./simulated_tg_channel -out ./restored_sample.iso

# 5. Check integrity
echo ""
echo "4. Comparing restored file size..."
ORIGINAL_SIZE=$(curl -s http://127.0.0.1:9099/info | grep -o '"size":[0-9]*' | cut -d: -f2)
RESTORED_SIZE=$(stat -c%s ./restored_sample.iso)

echo "Original size: $ORIGINAL_SIZE bytes"
echo "Restored size: $RESTORED_SIZE bytes"

if [ "$ORIGINAL_SIZE" -eq "$RESTORED_SIZE" ]; then
    echo "🎉 SUCCESS: File was perfectly downloaded via Range, chunked, transmitted, and reassembled!"
else
    echo "❌ Size mismatch!"
    exit 1
fi
