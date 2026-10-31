FROM golang:1.26.10-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /mgmt-mux ./cmd/mgmt-mux

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /mgmt-mux /usr/bin/mgmt-mux
COPY NOTICE /usr/share/doc/mgmt-mux/NOTICE
USER nonroot:nonroot
ENTRYPOINT ["/usr/bin/mgmt-mux"]
CMD ["-config", "/etc/mgmt-mux/config.yaml"]
