#!/bin/bash
# AWS CLI-based multipart upload integration test
# This script tests the complete multipart upload flow using AWS CLI

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
BUCKET="${TEST_BUCKET:-test-multipart}"
KEY="${TEST_KEY:-testfile.bin}"
ENDPOINT="${TEST_ENDPOINT:-http://localhost:8080}"
TEST_FILE="/tmp/testfile.bin"
# PART_SIZE will be computed based on requested file size and number of parts

# Defaults (can be overridden via env or stdin or CLI)
DEFAULT_SIZE_MB="${TEST_SIZE_MB:-15}"
DEFAULT_PARTS="${TEST_PARTS:-3}"

print_usage() {
    cat <<EOF
Usage: $0 [-s SIZE_MB] [-n PARTS]

You can pass parameters either via stdin (pipe) as: "<SIZE_MB> <PARTS>"
Or via command-line flags:
  -s SIZE_MB    Size of generated test file in MB (default: ${DEFAULT_SIZE_MB})
  -n PARTS      Number of parts to split into (default: ${DEFAULT_PARTS})

Environment variables:
  TEST_SIZE_MB and TEST_PARTS can also provide defaults.

Examples:
  echo "20 4" | $0        # create 20MB file split into 4 parts (from stdin)
  $0 -s 10 -n 2           # create 10MB file split into 2 parts (from flags)
EOF
}

# Read from stdin if piped (highest priority after CLI flags)
STDIN_SIZE_MB=""
STDIN_PARTS=""
if [ ! -t 0 ]; then
    # read up to two tokens from stdin
    read -r STDIN_SIZE_MB STDIN_PARTS || true
fi

# Parse CLI flags
CLI_SIZE_MB=""
CLI_PARTS=""
while getopts ":s:n:h" opt; do
  case $opt in
    s)
      CLI_SIZE_MB="$OPTARG"
      ;;
    n)
      CLI_PARTS="$OPTARG"
      ;;
    h)
      print_usage
      exit 0
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      print_usage
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      print_usage
      exit 1
      ;;
  esac
done

# Determine final values with priority: CLI > stdin > ENV/default
SIZE_MB="${CLI_SIZE_MB:-${STDIN_SIZE_MB:-$DEFAULT_SIZE_MB}}"
PARTS_COUNT="${CLI_PARTS:-${STDIN_PARTS:-$DEFAULT_PARTS}}"

# Basic validation: positive integers
re_digits='^[0-9]+$'
if ! [[ $SIZE_MB =~ $re_digits ]] || [ "$SIZE_MB" -le 0 ]; then
    echo -e "${RED}Invalid SIZE_MB: $SIZE_MB${NC}" >&2
    print_usage
    exit 1
fi
if ! [[ $PARTS_COUNT =~ $re_digits ]] || [ "$PARTS_COUNT" -le 0 ]; then
    echo -e "${RED}Invalid PARTS: $PARTS_COUNT${NC}" >&2
    print_usage
    exit 1
fi

# Compute sizes
SIZE_BYTES=$(( SIZE_MB * 1024 * 1024 ))
# ceil division for per-part size in bytes: ceil(SIZE_BYTES / PARTS_COUNT)
PART_SIZE=$(( (SIZE_BYTES + PARTS_COUNT - 1) / PARTS_COUNT ))

echo -e "${YELLOW}=== S3 Multipart Upload Integration Test ===${NC}"
echo "Bucket: $BUCKET"
echo "Key: $KEY"
echo "Endpoint: $ENDPOINT"
echo "Test file size: ${SIZE_MB} MB (${SIZE_BYTES} bytes)"
echo "Number of parts: ${PARTS_COUNT}"
echo "Per-part max size: ${PART_SIZE} bytes"
echo ""

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up test files...${NC}"
    rm -f /tmp/part-* /tmp/downloaded.bin $TEST_FILE /tmp/parts.json 2>/dev/null || true
}

trap cleanup EXIT

# Test 1: Create test bucket
echo -e "${YELLOW}[TEST 1] Creating test bucket...${NC}"
if aws s3 mb s3://$BUCKET --endpoint-url $ENDPOINT 2>/dev/null; then
    echo -e "${GREEN}✓ Bucket created successfully${NC}"
else
    echo -e "${YELLOW}⚠ Bucket already exists (continuing)${NC}"
fi

# Test 2: Create test file
echo -e "\n${YELLOW}[TEST 2] Creating ${SIZE_MB}MB test file...${NC}"
# Use dd to create the requested size in MB
dd if=/dev/urandom of=$TEST_FILE bs=1M count=$SIZE_MB 2>/dev/null
FILE_SIZE=$(stat -f%z "$TEST_FILE" 2>/dev/null || stat -c%s "$TEST_FILE")
echo -e "${GREEN}✓ Test file created: ${FILE_SIZE} bytes${NC}"

# Test 3: Split into parts
echo -e "\n${YELLOW}[TEST 3] Splitting file into ${PARTS_COUNT} parts (max ${PART_SIZE} bytes each)...${NC}"
split -b $PART_SIZE $TEST_FILE /tmp/part-
PART_COUNT=$(ls -1 /tmp/part-* | wc -l)
echo -e "${GREEN}✓ File split into ${PART_COUNT} parts${NC}"

# Test 4: Initiate multipart upload
echo -e "\n${YELLOW}[TEST 4] Initiating multipart upload...${NC}"
UPLOAD_ID=$(aws s3api create-multipart-upload \
    --bucket $BUCKET \
    --key $KEY \
    --endpoint-url $ENDPOINT \
    --output text \
    --query 'UploadId' 2>&1)

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to initiate multipart upload${NC}"
    echo "$UPLOAD_ID"
    exit 1
fi

echo -e "${GREEN}✓ Upload initiated with ID: ${UPLOAD_ID}${NC}"

# Test 5: Upload parts
echo -e "\n${YELLOW}[TEST 5] Uploading parts...${NC}"
PART_NUM=1
declare -a ETAGS
for PART_FILE in /tmp/part-*; do
    echo "  Uploading part ${PART_NUM}..."

    ETAG=$(aws s3api upload-part \
        --bucket $BUCKET \
        --key $KEY \
        --part-number $PART_NUM \
        --upload-id "$UPLOAD_ID" \
        --body "$PART_FILE" \
        --endpoint-url $ENDPOINT \
        --output text \
        --query 'ETag' 2>&1)

    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to upload part ${PART_NUM}${NC}"
        echo "$ETAG"

        # Cleanup: abort upload
        aws s3api abort-multipart-upload \
            --bucket $BUCKET \
            --key $KEY \
            --upload-id $UPLOAD_ID \
            --endpoint-url $ENDPOINT 2>/dev/null || true
        exit 1
    fi

    # Remove quotes from ETag
    ETAG=$(echo $ETAG | tr -d '"')
    ETAGS[$PART_NUM]=$ETAG
    echo -e "  ${GREEN}✓ Part ${PART_NUM} uploaded with ETag: ${ETAG}${NC}"

    PART_NUM=$((PART_NUM + 1))
done

# Test 6: List parts
echo -e "\n${YELLOW}[TEST 6] Listing uploaded parts...${NC}"
PARTS_LIST=$(aws s3api list-parts \
    --bucket $BUCKET \
    --key $KEY \
    --upload-id "$UPLOAD_ID" \
    --endpoint-url $ENDPOINT 2>&1)

# shellcheck disable=SC2181
if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to list parts${NC}"
    echo "$PARTS_LIST"
    exit 1
fi

LISTED_PARTS=$(echo "$PARTS_LIST" | grep -c "PartNumber" || echo "0")
echo -e "${GREEN}✓ Listed ${LISTED_PARTS} parts${NC}"

# Test 7: Complete multipart upload
echo -e "\n${YELLOW}[TEST 7] Completing multipart upload...${NC}"

# Build parts JSON
PARTS_JSON='{"Parts":['
for i in $(seq 1 $((PART_NUM - 1))); do
    if [ $i -gt 1 ]; then
        PARTS_JSON+=","
    fi
    PARTS_JSON+="{\"PartNumber\":$i,\"ETag\":\"${ETAGS[$i]}\"}"
done
PARTS_JSON+=']}'

echo "$PARTS_JSON" > /tmp/parts.json

COMPLETE_RESULT=$(aws s3api complete-multipart-upload \
    --bucket $BUCKET \
    --key $KEY \
    --upload-id "$UPLOAD_ID" \
    --multipart-upload "$PARTS_JSON" \
    --endpoint-url $ENDPOINT 2>&1)

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to complete multipart upload${NC}"
    echo "$COMPLETE_RESULT"
    exit 1
fi

FINAL_ETAG=$(echo "$COMPLETE_RESULT" | grep -o '"ETag"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)".*/\1/')
echo -e "${GREEN}✓ Multipart upload completed${NC}"
echo -e "  Final ETag: ${FINAL_ETAG}"

# Verify ETag format (should be {hash}-{part_count})
if [[ $FINAL_ETAG == *"-"* ]]; then
    PART_COUNT_IN_ETAG=$(echo $FINAL_ETAG | cut -d'-' -f2)
    echo -e "${GREEN}✓ ETag format correct (multipart with ${PART_COUNT_IN_ETAG} parts)${NC}"
else
    echo -e "${RED}✗ ETag format incorrect (expected {hash}-{count})${NC}"
    exit 1
fi

# Test 8: Download and verify
echo -e "\n${YELLOW}[TEST 8] Downloading and verifying object...${NC}"
aws s3 cp s3://$BUCKET/$KEY /tmp/downloaded.bin --endpoint-url $ENDPOINT --quiet

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to download object${NC}"
    exit 1
fi

DOWNLOADED_SIZE=$(stat -f%z "/tmp/downloaded.bin" 2>/dev/null || stat -c%s "/tmp/downloaded.bin")
echo -e "${GREEN}✓ Object downloaded: ${DOWNLOADED_SIZE} bytes${NC}"

# Verify file integrity
if diff $TEST_FILE /tmp/downloaded.bin > /dev/null 2>&1; then
    echo -e "${GREEN}✓ File integrity verified - download matches upload!${NC}"
else
    echo -e "${RED}✗ File integrity check failed - files differ${NC}"
    exit 1
fi

# Test 9: Verify .multipart directory cleaned up
echo -e "\n${YELLOW}[TEST 9] Verifying cleanup...${NC}"
# This test assumes we have access to the storage directory
# Adjust the path based on your configuration
STORAGE_DIR="${STORAGE_DIR:-./storage}"
if [ -d "$STORAGE_DIR/.multipart/$UPLOAD_ID" ]; then
    echo -e "${RED}✗ Multipart directory not cleaned up${NC}"
    exit 1
else
    echo -e "${GREEN}✓ Multipart directory cleaned up${NC}"
fi

# Test 10: Test abort multipart upload
echo -e "\n${YELLOW}[TEST 10] Testing abort multipart upload...${NC}"
ABORT_KEY="abort-test.bin"

# Initiate a new upload
ABORT_UPLOAD_ID=$(aws s3api create-multipart-upload \
    --bucket $BUCKET \
    --key $ABORT_KEY \
    --endpoint-url $ENDPOINT \
    --output text \
    --query 'UploadId')

echo "  Upload ID for abort test: $ABORT_UPLOAD_ID"

# Upload one part
aws s3api upload-part \
    --bucket $BUCKET \
    --key $ABORT_KEY \
    --part-number 1 \
    --upload-id "$ABORT_UPLOAD_ID" \
    --body /tmp/part-aa \
    --endpoint-url $ENDPOINT \
    --output text > /dev/null

# Abort the upload
aws s3api abort-multipart-upload \
    --bucket $BUCKET \
    --key $ABORT_KEY \
    --upload-id "$ABORT_UPLOAD_ID" \
    --endpoint-url $ENDPOINT

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Multipart upload aborted successfully${NC}"
else
    echo -e "${RED}✗ Failed to abort multipart upload${NC}"
    exit 1
fi

# Verify upload is gone
LIST_RESULT=$(aws s3api list-parts \
    --bucket $BUCKET \
    --key $ABORT_KEY \
    --upload-id "$ABORT_UPLOAD_ID" \
    --endpoint-url $ENDPOINT 2>&1 || true)

if echo "$LIST_RESULT" | grep -q "NoSuchUpload"; then
    echo -e "${GREEN}✓ Upload properly removed after abort${NC}"
else
    echo -e "${RED}✗ Upload still exists after abort${NC}"
fi

# Test 11: Test out-of-order part upload
echo -e "\n${YELLOW}[TEST 11] Testing out-of-order part upload...${NC}"
OOO_KEY="out-of-order.bin"

OOO_UPLOAD_ID=$(aws s3api create-multipart-upload \
    --bucket $BUCKET \
    --key $OOO_KEY \
    --endpoint-url $ENDPOINT \
    --output text \
    --query 'UploadId')

# Upload parts in reverse order (3, 2, 1)
declare -a OOO_ETAGS
for i in 3 2 1; do
    case $i in
        3) PART_FILE="/tmp/part-ac";;
        2) PART_FILE="/tmp/part-ab";;
        1) PART_FILE="/tmp/part-aa";;
    esac

    ETAG=$(aws s3api upload-part \
        --bucket $BUCKET \
        --key $OOO_KEY \
        --part-number $i \
        --upload-id "$OOO_UPLOAD_ID" \
        --body "$PART_FILE" \
        --endpoint-url $ENDPOINT \
        --output text \
        --query 'ETag' | tr -d '"')

    OOO_ETAGS[$i]=$ETAG
    echo "  Uploaded part $i (out of order)"
done

# Complete with parts in correct order
OOO_PARTS_JSON='{"Parts":['
for i in 1 2 3; do
    if [ $i -gt 1 ]; then
        OOO_PARTS_JSON+=","
    fi
    OOO_PARTS_JSON+="{\"PartNumber\":$i,\"ETag\":\"${OOO_ETAGS[$i]}\"}"
done
OOO_PARTS_JSON+=']}'

aws s3api complete-multipart-upload \
    --bucket $BUCKET \
    --key $OOO_KEY \
    --upload-id "$OOO_UPLOAD_ID" \
    --multipart-upload "$OOO_PARTS_JSON" \
    --endpoint-url $ENDPOINT > /dev/null

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Out-of-order upload completed successfully${NC}"
else
    echo -e "${RED}✗ Out-of-order upload failed${NC}"
    exit 1
fi

# Test 12: Cleanup - delete objects and bucket
echo -e "\n${YELLOW}[TEST 12] Cleaning up test bucket...${NC}"
aws s3 rm s3://$BUCKET/$KEY --endpoint-url $ENDPOINT --quiet
aws s3 rm s3://$BUCKET/$OOO_KEY --endpoint-url $ENDPOINT --quiet
aws s3 rb s3://$BUCKET --endpoint-url $ENDPOINT 2>/dev/null || true
echo -e "${GREEN}✓ Cleanup completed${NC}"

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}✓ ALL TESTS PASSED!${NC}"
echo -e "${GREEN}========================================${NC}"
