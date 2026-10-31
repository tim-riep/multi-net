// Command mgmt-mux lets one NetBird reverse proxy serve several management
// servers by presenting them to the proxy as a single management.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/tim-riep/multi-net/multi/mux/internal/mux"
)

func main() {
	configPath := flag.String("config", envOr("MUX_CONFIG", "/etc/mgmt-mux/config.yaml"), "path to the config file")
	logLevel := flag.String("log-level", envOr("MUX_LOG_LEVEL", "info"), "log level")
	flag.Parse()

	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("invalid log level: %v", err)
	}
	log.SetLevel(lvl)
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})

	cfg, err := mux.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := mux.Run(ctx, cfg, log.NewEntry(log.StandardLogger())); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
