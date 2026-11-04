# Build stage: runs natively on the build host and cross-compiles for the target platform
FROM --platform=$BUILDPLATFORM golang:1.26.10-alpine AS builder

ARG TARGETOS TARGETARCH

# Set working directory
WORKDIR /app

COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the entire worker source code
COPY . ./

# Cross-compile on the build host instead of under QEMU emulation.
# -trimpath drops local paths, -s -w drop the symbol table and DWARF data (stack traces still work).
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o olake-worker ./main.go

# Prepare the runtime directories here, so the runtime stage needs no RUN step
RUN mkdir -p /rootfs/mnt/config /rootfs/tmp/olake-config

# Runtime stage: COPY steps only, so multi-arch builds do not need QEMU.
# Alpine is kept (not scratch) because the Helm chart's worker probes run /bin/sh and wget,
# and the base image already ships the CA bundle at /etc/ssl/certs/ca-certificates.crt.
FROM alpine:3.24

# Create app directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/olake-worker .

# Copy the necessary directories. COPY does not keep the source directory's mode, so set it here.
COPY --from=builder --chmod=777 /rootfs/mnt/config /mnt/config
COPY --from=builder --chmod=777 /rootfs/tmp/olake-config /tmp/olake-config

# Health server port (/health, /ready, /metrics)
EXPOSE 8090

# Run the worker
CMD ["./olake-worker"]
