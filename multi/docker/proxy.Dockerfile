# Builds the reverse proxy of this fork. Cross-compiles on the build
# platform, so multi-arch builds need no emulation.
FROM --platform=$BUILDPLATFORM golang:1.26.10-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=development
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags="-s -w -X github.com/netbirdio/netbird/version.version=${VERSION}" \
      -o /out/multi-proxy ./proxy/cmd/proxy

RUN echo "proxy:x:1000:1000:proxy:/var/lib/proxy:/sbin/nologin" > /tmp/passwd && \
    echo "proxy:x:1000:proxy" > /tmp/group && \
    mkdir -p /tmp/var/lib/proxy /tmp/certs

FROM gcr.io/distroless/base:debug
ARG VERSION=development
LABEL org.opencontainers.image.title="multi-proxy" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="AGPL-3.0-only"
COPY --from=builder /out/multi-proxy /usr/bin/multi-proxy
COPY --from=builder /tmp/passwd /etc/passwd
COPY --from=builder /tmp/group /etc/group
COPY --from=builder --chown=1000:1000 /tmp/var/lib/proxy /var/lib/proxy
COPY --from=builder --chown=1000:1000 --chmod=755 /tmp/certs /certs
USER proxy:proxy
ENV HOME=/var/lib/proxy
ENV NB_PROXY_ADDRESS=":8443"
EXPOSE 8443
ENTRYPOINT ["/usr/bin/multi-proxy"]
