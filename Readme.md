# S3-Storage

A Go application providing an S3-compatible object storage server with a web UI for managing buckets, objects, users, and permissions.

**Key Features:**
- S3-compatible API (AWS SDK compatible)
- Web-based UI for bucket and object management
- Per-user S3 credentials and ACLs
- AWS SigV4 authentication
- Content-type detection with Magika (Google's ML model)
- Storage quotas and monitoring
- Share links with expiration
- Bulk operations (upload, delete)
- Telemetry and observability

# Magika and ONNXRUNTIME Integration

This project uses **ONNXRUNTIME** for running the **Magika** ML model (Google's content-type detection).

## Automatic Setup (Recommended)

**The Taskfile automatically downloads all dependencies** - no manual installation needed!

```bash
cd src
task build  # Downloads ONNXRUNTIME + Magika automatically, then builds
```

On first run, Task will:
1. Detect your OS and architecture (macOS x64/ARM64, Linux x64/ARM64)
2. Download the correct ONNXRUNTIME release (v1.23.2)
3. Clone Magika repository (shallow clone)
4. Extract to `.deps/` directory (git-ignored)
5. Build the binary

**Supported Platforms:**
- macOS Intel (x86_64) → `onnxruntime-osx-x86_64-1.23.2`
- macOS Apple Silicon (arm64) → `onnxruntime-osx-arm64-1.23.2`
- Linux x64 (x86_64) → `onnxruntime-linux-x64-1.23.2`
- Linux ARM64 (aarch64) → `onnxruntime-linux-aarch64-1.23.2`

Dependencies are cached in `.deps/` and only downloaded once:
```
go-s3-storage/
├── .deps/                    # Auto-created, git-ignored
│   ├── onnxruntime/         # Auto-downloaded
│   │   ├── include/
│   │   └── lib/
│   └── magika/              # Auto-cloned
│       └── assets/
└── src/
```

### Available Setup Tasks

```bash
task setup              # Setup all dependencies (ONNXRUNTIME + Magika)
task setup-onnxruntime  # Setup only ONNXRUNTIME
task setup-magika       # Setup only Magika
task clean-deps         # Remove downloaded dependencies
```

## Manual Installation (Optional)

If you prefer to manage dependencies manually or have them installed system-wide:

### Prerequisites

#### macOS - Install xcode-select

```bash
xcode-select --install
clang --version
```

#### Download ONNXRUNTIME Manually

Choose the version for your platform:

**macOS Intel:**
```bash
wget https://github.com/microsoft/onnxruntime/releases/download/v1.23.2/onnxruntime-osx-x86_64-1.23.2.tgz
tar -xzf onnxruntime-osx-x86_64-1.23.2.tgz
```

**macOS Apple Silicon:**
```bash
wget https://github.com/microsoft/onnxruntime/releases/download/v1.23.2/onnxruntime-osx-arm64-1.23.2.tgz
tar -xzf onnxruntime-osx-arm64-1.23.2.tgz
```

**Linux x64:**
```bash
wget https://github.com/microsoft/onnxruntime/releases/download/v1.23.2/onnxruntime-linux-x64-1.23.2.tgz
tar -xzf onnxruntime-linux-x64-1.23.2.tgz
```

#### Clone Magika Manually

```bash
git clone --depth=1 https://github.com/google/magika.git
```

### Set Environment Variables

If using manual installation, configure these environment variables **before building**:

```bash
# Point to your ONNXRUNTIME installation
xport CGO_ENABLED=1
export CGO_CFLAGS="-I/path/to/onnxruntime/include"
export DYLD_LIBRARY_PATH="/path/to/onnxruntime/lib"
export ONNXRUNTIME_LIB="/path/to/onnxruntime/lib"
export ONNXRUNTIME_INCLUDE="/path/to/onnxruntime/include"
# Point to your Magika installation
export MAGIKA_ASSETS_DIR="/path/to/magika/assets"

# Example:
export CGO_ENABLED=1
export CGO_CFLAGS="-I/opt/onnxruntime/include"
export DYLD_LIBRARY_PATH="/opt/onnxruntime/lib"
export ONNXRUNTIME_LIB="/opt/onnxruntime-osx-arm64-1.23.2/lib"
export ONNXRUNTIME_INCLUDE="/opt/onnxruntime-osx-arm64-1.23.2/include"
export MAGIKA_ASSETS_DIR="/opt/magika/assets"
```

Then build normally:
```bash
cd src
task build  # Uses your custom paths
```

__!!!NB!!!__ The Taskfile will detect these environment variables and skip automatic downloads.

# Building It Locally

After cloning the repo, you have several options:

## Option 1: Build with Task (Recommended)

**Easiest method** - automatically downloads all dependencies:

```shell
cd src
task build
```

This will:
1. Download ONNXRUNTIME (correct version for your OS/architecture)
2. Clone Magika repository
3. Download Tailwind CSS binary
4. Generate Swagger docs
5. Run static analysis
6. Build the binary

**First build:**
```
Setting up ONNXRUNTIME...
Downloading ONNXRUNTIME osx-arm64 version 1.23.2...
Extracting...
✓ ONNXRUNTIME installed to ../.deps/onnxruntime
Setting up Magika...
Cloning Magika repository...
✓ Magika cloned to ../.deps/magika
Building Tailwind CSS...
Generating Swagger documentation...
Running static analysis...
Building binary...
Using ONNXRUNTIME lib: ../.deps/onnxruntime/lib
```

**Subsequent builds** (dependencies cached):
```
ONNXRUNTIME already installed at ../.deps/onnxruntime
Magika already cloned at ../.deps/magika
Building binary...
```

## Option 2: Build Manually with Go

If you have ONNXRUNTIME and Magika installed manually:

```shell
cd src
export CGO_ENABLED=1
export CGO_CFLAGS="-I/path/to/onnxruntime/include"
export DYLD_LIBRARY_PATH="/path/to/onnxruntime/lib"
export MAGIKA_ASSETS_DIR=/path/tomagika/assets
export ONNXRUNTIME_LIB="/path/to/onnxruntime/lib"
export ONNXRUNTIME_INCLUDE="/path/to/onnxruntime/include"
export MAGIKA_ASSETS_DIR="/path/to/magika/assets"

go build -tags onnxruntime \
  -ldflags="-linkmode=external -extldflags=-L${ONNXRUNTIME_LIB}" \
  main.go
```

## Option 3: Build the Docker Image

```shell
cd $GOPATH/src
docker build -t s3-storage -f go-s3-storage/Dockerfile .
```

# Running It Locally

If you built it locally, then execute the binary and pass necessary command-line parameters.

```shell
./s3-storage [opts]
```

## Using Taskfile

This project uses [Task](https://taskfile.dev/) for task automation.

### Install Task

```shell
brew install go-task/tap/go-task
```

### List Available Tasks

```shell
task --list
```

### Available Tasks

```shell
# Dependency Management
task setup              # Setup all dependencies (ONNXRUNTIME + Magika)
task setup-onnxruntime  # Setup only ONNXRUNTIME
task setup-magika       # Setup only Magika
task clean-deps         # Remove downloaded dependencies

# Development
task deps      # Download Go dependencies
task vet       # Run static code analysis
task test      # Run unit tests
task coverage  # Generate code coverage report
task swagger   # Generate Swagger documentation
task css       # Build Tailwind CSS

# Build & Run
task build     # Build the Go binary (auto-downloads deps if needed)
task run       # Build and run the application
task docker-build  # Build Docker container

# Cleanup
task clean     # Clean up build and generated files
```

### Quick Start

**Build and run in one command:**

```shell
cd src
task run  # Automatically downloads deps, builds, and runs
```

Or separately:

```shell
cd src
task build  # Download deps and build
task run    # Run the application
```

### Run Default Task

```shell
task  # Same as 'task run'
```

### First Time Setup

On a clean machine with no dependencies installed:

```shell
git clone <repository>
cd go-s3-storage/src
task build
```

That's it! No manual dependency installation needed.

### Clean Rebuild

To force re-download of dependencies:

```shell
task clean-deps  # Remove .deps/ directory
task build       # Re-download and build
```

Refer to `src/Taskfile.yaml` for all available tasks and their descriptions.

# Running with Docker

If you built the Docker image, you can run it with:

```shell
docker run --rm -it \
  -p 8080:8080 \
  -v /path/to/data:/data \
  -e S3_AUTH_ENABLED=true \
  -e S3_ACCESS_KEY_ID=your-access-key \
  -e S3_SECRET_ACCESS_KEY=your-secret-key \
  -e WEB_UI_ENABLED=true \
  -e LOCAL_AUTH_USERNAME=admin \
  -e LOCAL_AUTH_PASSWORD=changeme123 \
  s3-storage
```

# Running with Nomad

You can deploy the application using [HashiCorp Nomad](https://www.nomadproject.io/).

1. Ensure Nomad is installed and running on your system.
2. Prepare your job file (e.g., `s3-storage.nomad`).
3. Run the job:

```shell
nomad job run s3-storage.nomad
```

4. Check job status:

```shell
nomad job status go-s3-storage
```

5. To stop the job:

```shell
nomad job stop go-s3-storage
```

**Note:**
Make sure all required environment variables and Docker images are available to Nomad before running the job.

# Command Line Parameters

You may specify a number of command-line parameters which change the behavior of the application:

| Short | Long         | Default | Usable in prod | Description                                                                                                       |
|-------|--------------|---------|----------------|-------------------------------------------------------------------------------------------------------------------|
| -t    | --timeout    | 60      | Yes            | Time to wait for graceful shutdown on SIGTERM/SIGINT in seconds                                                   |
| -p    | --port       | 8080    | Yes            | TCP port for the HTTP listener to bind to                                                                         |
| -s    | --swagger    |         | No             | Activate swagger. Do not use this in Production!                                                                  |
| -d    | --devel      |         | No             | Start in development mode. Implies --swagger. Do not use this in Production!                                      |
| -g    | --gin-logger |         | No             | Activate Gin's logger, for debugging. **Warning**: This breaks structured logging. Do not use this in Production! |
| -r    | --telemetry  |         | Yes            | Enable telemetry. Values accepted: local (for local telemetry) remote (for Jaeger telemetry)                      |
| -o    | --pprof      |         | Yes            | Enable pprof endpoints for profiling (at /debug/pprof/)                                                            |

# Environment Variables and Options

## Core Settings

```shell
# Application environment
export ENVIRONMENT="production"

# HTTP server port (default: 8080)
export HTTP_PORT=8080

# Request base URL (used for generating share links)
export REQUEST_BASE_URL="https://s3-storage.almeriaindustries.com"

# CORS allow origins (comma-separated)
export CORS_ALLOW_ORIGINS="https://example.com,https://another.com"

# Shutdown timeout in seconds
export SHUTDOWN_TIMEOUT=300
```

## Storage Settings

```shell
# Storage directory (where buckets and objects are stored)
export STORAGE_DIRECTORY="/data"

# Storage quota in bytes (0 for unlimited)
export STORAGE_QUOTA_BYTES=10737418240  # 10GB
```

## S3 Authentication

```shell
# Enable S3 authentication
export S3_AUTH_ENABLED="true"

# AWS region (used for signature verification)
export S3_AUTH_REGION="us-east-1"

# Bootstrap admin S3 credentials (global credentials for initial admin)
export S3_ACCESS_KEY_ID="your-access-key-id"
export S3_SECRET_ACCESS_KEY="your-secret-access-key"
```

__!!!NB!!!__ The bootstrap admin uses these global credentials. Regular users get their own credentials set by the admin through the Web UI.

## Magika (Content-Type Detection)

```shell
# Path to Magika assets directory
export MAGIKA_ASSETS_DIR="/opt/magika/assets"

# Magika model name (default: standard_v3_3)
export MAGIKA_MODEL_NAME="standard_v3_3"
```

## Web UI

```shell
# Enable Web UI
export WEB_UI_ENABLED="true"

# Web UI path prefix (default: /ui)
export WEB_UI_PREFIX="/ui"

# Session secret for cookies (must be 32 bytes for AES-256)
export SESSION_SECRET="change-me-to-32-byte-secret-key!"

# Session TTL in seconds (default: 86400 = 24 hours)
export SESSION_TTL=86400

# Objects per page in UI (default: 50)
export UI_OBJECTS_PER_PAGE=50

# Maximum objects per page (default: 500)
export UI_MAX_OBJECTS_PER_PAGE=500
```

## Local Authentication (Bootstrap Admin)

```shell
# Enable local authentication
export LOCAL_AUTH_ENABLED="true"

# Bootstrap admin username
export LOCAL_AUTH_USERNAME="admin"

# Bootstrap admin password
export LOCAL_AUTH_PASSWORD="changeme123"
```

## Telemetry (Jaeger)

For telemetry using Jaeger, the app requires Jaeger endpoint:

```shell
export JAEGER_ENDPOINT="localhost:4318"
```

### Starting Local Jaeger Server

```shell
docker run -d --name jaeger \
  -e COLLECTOR_ZIPKIN_HOST_PORT=:9411 \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 5775:5775/udp \
  -p 6831:6831/udp \
  -p 6832:6832/udp \
  -p 5778:5778 \
  -p 16686:16686 \
  -p 14250:14250 \
  -p 14268:14268 \
  -p 14269:14269 \
  -p 9411:9411 \
  -p 4318:4318 \
  jaegertracing/all-in-one:latest
```

(open browser: http://localhost:16686/)

## Rate Limiting

```shell
# Maximum events per second (default: 100)
export MAX_EVENTS_PER_SEC=100

# Maximum burst size (default: 120)
export MAX_BURST_SIZE=120
```

## Ingress/Reverse Proxy

```shell
# Ingress host (for Traefik/nginx)
export INGRESS_HOST="s3-storage"

# Ingress path prefix
export INGRESS_PREFIX=""
```

# Features

## S3 API Compatibility

The server implements the core S3 API operations:

- **Buckets**: CreateBucket, DeleteBucket, ListBuckets, HeadBucket
- **Objects**: PutObject, GetObject, DeleteObject, HeadObject, ListObjectsV2
- **Authentication**: AWS Signature Version 4 (SigV4)

Compatible with AWS SDKs (boto3, aws-sdk-go, aws-sdk-js, etc.)

## Web UI

Access the web UI at: `http://localhost:8080/ui`

Features:
- Dashboard with storage statistics
- Bucket management (create, delete, configure ACLs)
- Object browser (upload, download, delete, bulk operations)
- User management (create users, set S3 credentials, manage roles)
- Share link generation with expiration
- Object signature (SHA256) display and copy

## Per-User S3 Credentials

- Admin users can set S3 credentials (Access Key ID + Secret Access Key) for each user
- Each user authenticates with their own credentials
- Bootstrap admin uses global credentials from environment variables
- Credentials are stored securely in the `.users` file

## Access Control

- **Admin users**: Full access to all buckets and objects
- **Regular users**: Access to owned buckets + buckets with explicit permissions
- **Bucket ACLs**: Configure read/write permissions per user

## Storage Quotas

Set storage limits per deployment:

```shell
export STORAGE_QUOTA_BYTES=10737418240  # 10GB
```

The system prevents uploads that would exceed the quota.

## Share Links

Generate temporary share links for objects with configurable expiration:
- 1 hour
- 1 day
- 1 week
- 30 days
- Never expires

Share links are accessible without authentication.

# API Documentation

All endpoints are documented using [Swagger](http://localhost:8080/swagger/index.html)

To enable Swagger:

```shell
./s3-storage -d -s
```

(open browser: http://localhost:8080/swagger/index.html)

# Swagger for Development

First, get the swagger/swag Go application:

```shell
go install github.com/swaggo/swag/cmd/swag@latest
```

Now, every time you make a change to the Swagger headers, you will need to regenerate the docs:

```shell
cd src
swag init --parseDependency
```

If you have a problem with your headers or mappings, you will get an error describing what's wrong. You **must** fix these before committing the code!

**Note:** The docs are regenerated automatically when building with `task build`.

# API Request Examples

## Using AWS CLI

### Configure AWS CLI

```shell
aws configure set aws_access_key_id your-access-key-id
aws configure set aws_secret_access_key your-secret-access-key
aws configure set default.region us-east-1
```

### Create a Bucket

```shell
aws --endpoint-url=http://localhost:8080 s3 mb s3://my-bucket
```

### Upload a File

```shell
aws --endpoint-url=http://localhost:8080 s3 cp myfile.txt s3://my-bucket/
```

### List Objects

```shell
aws --endpoint-url=http://localhost:8080 s3 ls s3://my-bucket/
```

### Download a File

```shell
aws --endpoint-url=http://localhost:8080 s3 cp s3://my-bucket/myfile.txt ./downloaded.txt
```

### Delete an Object

```shell
aws --endpoint-url=http://localhost:8080 s3 rm s3://my-bucket/myfile.txt
```

## Using boto3 (Python)

```python
import boto3

# Create S3 client
s3 = boto3.client('s3',
    endpoint_url='http://localhost:8080',
    aws_access_key_id='your-access-key-id',
    aws_secret_access_key='your-secret-access-key',
    region_name='us-east-1'
)

# Create bucket
s3.create_bucket(Bucket='my-bucket')

# Upload file
with open('myfile.txt', 'rb') as f:
    s3.put_object(Bucket='my-bucket', Key='myfile.txt', Body=f)

# List objects
response = s3.list_objects_v2(Bucket='my-bucket')
for obj in response.get('Contents', []):
    print(obj['Key'])

# Download file
s3.download_file('my-bucket', 'myfile.txt', 'downloaded.txt')

# Delete object
s3.delete_object(Bucket='my-bucket', Key='myfile.txt')
```

## Using cURL (Direct S3 API)

### Create Bucket

```shell
curl -X PUT http://localhost:8080/my-bucket \
  -H "Authorization: AWS4-HMAC-SHA256 ..."
```

### Upload Object

```shell
curl -X PUT http://localhost:8080/my-bucket/myfile.txt \
  -H "Authorization: AWS4-HMAC-SHA256 ..." \
  -H "X-Amz-Content-SHA256: <sha256-hash>" \
  --data-binary @myfile.txt
```

__!!!NB!!!__ For simplicity, use AWS CLI or SDK instead of manually crafting SigV4 signatures.

# Web UI Examples

## Login

1. Navigate to `http://localhost:8080/ui`
2. Enter username and password (default: admin / changeme123)
3. Click "Sign In"

## Create a Bucket

1. Go to Buckets page
2. Click "Create Bucket"
3. Enter bucket name
4. Click "Create"

## Upload Files

1. Navigate to a bucket
2. Click "Upload" button
3. Drag and drop files or click to select
4. Files upload automatically

## Set S3 Credentials for a User

1. Go to Users page (admin only)
2. Click "S3 Credentials" button for a user
3. Enter Access Key ID and Secret Access Key
4. Click "Save"

## Create Share Link

1. Navigate to an object
2. Click "Share" button
3. Select expiration time
4. Click "Create Link"
5. Copy the generated URL

# Deployment Behind Cloudflare/Traefik

The application is designed to work behind reverse proxies (Cloudflare, Traefik, nginx):

- Automatically detects proxy headers (CF-Ray, X-Forwarded-For, X-Real-IP)
- Adjusts signature validation for proxied requests
- Handles chunked transfer encoding
- Supports large file uploads (up to 100MB per request with Cloudflare)

__!!!NB!!!__ Cloudflare has a 100MB upload limit on the free plan. For larger files, consider Cloudflare Workers or direct uploads.

# Run Functional Tests

__!!!NB!!!__ Tests require the application to be running locally.

## Run All Tests

```shell
cd test
go test -run ''
```

## Run a Specific Test Suite

```shell
cd test
go test -run TestBuckets
```

## Run a Specific Test

```shell
cd test
go test -run TestBuckets/CreateBucket
```

## Run multipart tests

```shell
cd src
go test ./services -run Multipart -v     # Run all service multipart tests with verbose output                                                                                                                                                                                           
go test ./api/handlers -run Multipart -v # Run all handler multipart tests with verbose output                                                                                                                                                                                            
go test ./... -run Multipart -v          # Run all multipart tests everywhere                                                                                                                                                                                          
```



```shell

## Test Endpoint Configuration

Configure the test endpoint via environment variable:

```shell
export TEST_ENDPOINT="http://localhost:8080/"
```

# Architecture

## Components

- **Gin HTTP Server**: REST API and Web UI routing
- **VFS Layer**: Virtual filesystem for bucket/object storage
- **MetaStore**: Metadata management (xl.meta files)
- **UserService**: User authentication and authorization
- **CredentialStore**: S3 credential management
- **Magika Scanner**: ML-based content-type detection
- **Session Manager**: Web UI session management
- **ShareLink Manager**: Temporary URL generation

## Storage Layout

```
/data/
├── .users              # User database (JSON)
├── .sharelinks         # Share links database (JSON)
├── bucket1/            # Bucket directory
│   ├── .bucket.meta    # Bucket metadata
│   └── object1/        # Object directory
│       ├── xl.meta     # Object metadata (msgpack)
│       └── part.1      # Object data
└── bucket2/
    └── ...
```

## Dependency Management

The project uses **automatic dependency downloads** similar to Docker multi-stage builds:

### Local Development (Taskfile)

Dependencies are automatically downloaded to `.deps/` (git-ignored):

```
go-s3-storage/
├── .deps/                          # Auto-created (git-ignored)
│   ├── onnxruntime/               # v1.23.2 for your OS/arch
│   │   ├── include/               # C headers
│   │   └── lib/                   # Shared libraries (.dylib/.so)
│   └── magika/                    # Google Magika (shallow clone)
│       └── assets/                # ML model files
│           └── standard_v3_3.onnx
└── src/
    ├── Taskfile.yaml              # Handles automatic downloads
    └── main.go
```

**How it works:**

1. `task build` runs → checks if `.deps/` exists
2. If not → detects OS/architecture
3. Downloads correct ONNXRUNTIME release from GitHub
4. Clones Magika repository (shallow)
5. Caches in `.deps/` for subsequent builds
6. Sets environment variables dynamically
7. Builds binary with correct linker flags

**Supported platforms:**
- macOS Intel (x86_64)
- macOS Apple Silicon (arm64)
- Linux x64 (x86_64)
- Linux ARM64 (aarch64)

**Override behavior:**

Set environment variables to use custom installations:
```bash
export ONNXRUNTIME_LIB="/custom/path/lib"
export ONNXRUNTIME_INCLUDE="/custom/path/include"
export MAGIKA_ASSETS="/custom/magika/assets"
```

Taskfile detects these and skips downloads.

### Docker Build

Docker uses multi-stage builds to download dependencies during image creation:

1. **Build stage**: Downloads ONNXRUNTIME + Magika, compiles Go binary
2. **Runtime stage**: Only copies necessary libraries and assets (minimal image)

See `Dockerfile` for details.

## Security

- AWS SigV4 signature verification for S3 API
- CSRF protection for Web UI
- Session-based authentication with secure cookies
- Password hashing with bcrypt
- CORS configuration
- Rate limiting

# Troubleshooting

## Error: "makeslice: cap out of range"

**Cause**: Chunked transfer encoding (ContentLength = -1) when uploading through Cloudflare.

**Solution**: This is already fixed in the current version. The application handles chunked encoding automatically.

## Error: "SignatureDoesNotMatch - payload hash mismatch"

**Cause**: Request body modified by reverse proxy (Cloudflare, Traefik).

**Solution**: This is already fixed. The application detects proxied requests and skips strict payload validation.

## Uploads Timeout with 502 Error

**Cause**: Files larger than 100MB through Cloudflare, or insufficient memory.

**Solution**:
- Cloudflare free plan limits uploads to 100MB
- Use Cloudflare Workers for larger files
- Or bypass Cloudflare for S3 API (use direct IP/domain)

## Cannot Login to Web UI

**Cause**: Incorrect username/password or session secret changed.

**Solution**:
- Check `LOCAL_AUTH_USERNAME` and `LOCAL_AUTH_PASSWORD` environment variables
- Default is `admin` / `changeme123`
- Clear browser cookies if session secret was changed

## Dependency Download Failures

### Error: "Failed to download ONNXRUNTIME"

**Cause**: Network issues or GitHub releases unavailable.

**Solution**:
1. Check your internet connection
2. Try downloading manually from GitHub releases:
   ```shell
   curl -L https://github.com/microsoft/onnxruntime/releases/download/v1.23.2/onnxruntime-osx-arm64-1.23.2.tgz -o onnxruntime.tgz
   ```
3. Or provide your own installation:
   ```shell
   export ONNXRUNTIME_LIB="/path/to/onnxruntime/lib"
   export ONNXRUNTIME_INCLUDE="/path/to/onnxruntime/include"
   task build
   ```

### Error: "Magika Model Not Found" or "No such file or directory: .deps/magika/assets"

**Cause**: Magika repository failed to clone or `MAGIKA_ASSETS_DIR` incorrect.

**Solution 1 - Let Task re-download:**
```shell
task clean-deps
task setup-magika
```

**Solution 2 - Manual clone:**
```shell
mkdir -p .deps
git clone --depth=1 https://github.com/google/magika.git .deps/magika
task build
```

**Solution 3 - Use custom path:**
```shell
export MAGIKA_ASSETS="/path/to/magika/assets"
task build
```

### Dependencies Not Found After Download

**Cause**: Built in wrong directory or `.deps/` was deleted.

**Solution**:
```shell
cd src  # Make sure you're in the src directory
task clean-deps
task build  # Re-download dependencies
```

### Using Custom Dependency Paths

If you already have ONNXRUNTIME or Magika installed:

```shell
# Set environment variables before building
export ONNXRUNTIME_LIB="/opt/onnxruntime/lib"
export ONNXRUNTIME_INCLUDE="/opt/onnxruntime/include"
export MAGIKA_ASSETS="/opt/magika/assets"

cd src
task build  # Will use your custom paths
```

The Taskfile will detect these variables and skip automatic downloads.

# License

Copyright © 2024 Almeria Industries

# Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `task test`
5. Submit a pull request

# Support

For issues and questions:
- GitHub Issues: https://github.com/your-org/go-s3-storage/issues
- Email: support@almeriaindustries.com
