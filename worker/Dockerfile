# Build stage
FROM --platform=$BUILDPLATFORM golang:1.26.9-alpine AS builder

ARG TARGETOS TARGETARCH

# Set working directory
WORKDIR /app

COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the entire worker source code
COPY . ./

# Cross-compile on the build host instead of under QEMU emulation
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o olake-worker ./main.go

# Runtime stage
FROM alpine:latest

# Install required packages
RUN apk update && apk add --no-cache \
    docker-cli \
    ca-certificates

# Create app directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/olake-worker .

# Create necessary directories with proper permissions
RUN mkdir -p /mnt/config && chmod -R 777 /mnt/config
RUN mkdir -p /tmp/olake-config && chmod -R 777 /tmp/olake-config

EXPOSE 8080

# Run the worker
CMD ["./olake-worker"]