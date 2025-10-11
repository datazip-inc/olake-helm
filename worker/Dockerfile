# Build stage
FROM golang:1.24.2-alpine AS builder

# Set working directory
WORKDIR /app

COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the entire worker source code
COPY . ./

# Build the worker binary
RUN go build -o olake-worker ./main.go

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