FROM golang:1.25 AS build

WORKDIR /tmp

# ----------------------------
# Detect architecture
# ----------------------------
ARG TARGETARCH
ARG ONNX_NAME=onnxruntime
ARG ONNX_VERSION=1.23.2

# Map Docker arch → ONNX arch
RUN case "${TARGETARCH}" in \
      "amd64")  export ONNX_ARCH="linux-x64" ;; \
      "arm64")  export ONNX_ARCH="linux-aarch64" ;; \
      *) echo "Unsupported arch: ${TARGETARCH}" && exit 1 ;; \
    esac && \
    ONNX_FULLNAME=${ONNX_NAME}-${ONNX_ARCH}-${ONNX_VERSION} && \
    ONNX_TARBALL=${ONNX_FULLNAME}.tgz && \
    curl -sL -O https://github.com/microsoft/${ONNX_NAME}/releases/download/v${ONNX_VERSION}/${ONNX_TARBALL} && \
    tar -xzf ${ONNX_TARBALL} -C /opt && \
    ln -s /opt/${ONNX_FULLNAME} /opt/onnxruntime

# ----------------------------
# Fetch Magika assets
# ----------------------------
RUN git clone --depth=1 https://github.com/google/magika.git /tmp/magika

# ----------------------------
# Build Go binary
# ----------------------------
WORKDIR /go-s3-storage/src
ADD go-s3-storage /go-s3-storage

ARG CGO_ENABLED=1
ARG CGO_CFLAGS=-I/opt/onnxruntime/include
ARG LD_LIBRARY_PATH=/opt/onnxruntime/lib

RUN go get -v ./... \
    && go build -tags onnxruntime \
       -ldflags="-linkmode=external -extldflags=-L/opt/onnxruntime/lib" \
       -o /s3-storage main.go

# ----------------------------
# Runtime stage
# ----------------------------
FROM gcr.io/distroless/cc

ENV LD_LIBRARY_PATH=/opt/onnxruntime/lib
ENV MAGIKA_ASSETS_DIR=/opt/magika/assets
ENV MAGIKA_MODEL=standard_v3_3

COPY --from=build /opt/onnxruntime/lib /opt/onnxruntime/lib
COPY --from=build /tmp/magika/assets /opt/magika/assets
COPY --from=build /s3-storage /

ADD go-s3-storage/src/swagger.yaml /swagger.yaml

EXPOSE 8080 53835
ENTRYPOINT ["/s3-storage"]