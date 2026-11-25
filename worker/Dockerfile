# The worker binary is prebuilt by `make dist`. This file only copies it in.

# The runtime directories are the same on every arch, so build them on the host arch.
FROM --platform=$BUILDPLATFORM alpine:3.24 AS rootfs
RUN mkdir -p /rootfs/mnt/config /rootfs/tmp/olake-config \
 && chmod 777 /rootfs/mnt/config /rootfs/tmp/olake-config \
 && chmod 1777 /rootfs/tmp


# Alpine, not scratch: the Helm chart's worker probes run /bin/sh and wget, and the
# base image already ships the CA bundle at /etc/ssl/certs/ca-certificates.crt.
FROM alpine:3.24

COPY --from=rootfs /rootfs/ /

ARG TARGETOS
ARG TARGETARCH
# chmod: the binaries come through a workflow artifact, which drops the exec bit.
COPY --chmod=0755 dist/${TARGETOS}/${TARGETARCH}/olake-worker /app/olake-worker

WORKDIR /app

# Health server port (/health, /ready, /metrics)
EXPOSE 8090

# Runs as root: the worker needs the mounted docker.sock.
CMD ["./olake-worker"]
