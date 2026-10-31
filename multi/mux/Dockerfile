FROM --platform=$BUILDPLATFORM golang:1.26.10-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/mgmt-mux ./cmd/mgmt-mux

FROM gcr.io/distroless/static:nonroot
ARG VERSION=development
LABEL org.opencontainers.image.title="mgmt-mux" \
      org.opencontainers.image.version="${VERSION}"
COPY --from=builder /out/mgmt-mux /usr/bin/mgmt-mux
COPY NOTICE /usr/share/doc/mgmt-mux/NOTICE
USER nonroot:nonroot
ENTRYPOINT ["/usr/bin/mgmt-mux"]
CMD ["-config", "/etc/mgmt-mux/config.yaml"]
