package mux

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

const defaultInitialSyncTimeout = 30 * time.Second

// Config is the multiplexer configuration file.
type Config struct {
	// Listen is the gRPC address the proxy connects to (its -mgmt flag).
	Listen string `yaml:"listen"`
	// ProxyToken, if set, must be presented by the proxy as NB_PROXY_TOKEN.
	ProxyToken string `yaml:"proxy_token"`
	// TLSCertFile and TLSKeyFile enable TLS on Listen.
	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`
	// StatusListen, if set, serves a JSON status page at /status.
	StatusListen string `yaml:"status_listen"`
	// InitialSyncTimeout bounds how long the first snapshot to the proxy
	// waits for all managements to finish their initial sync.
	InitialSyncTimeout time.Duration `yaml:"initial_sync_timeout"`
	// AllowDirectUpstream lets managements use direct upstream targets
	// (including proxy cluster targets), which the proxy dials from its own
	// host network instead of through the tunnel. Off for a shared proxy.
	AllowDirectUpstream bool         `yaml:"allow_direct_upstream"`
	Managements         []Management `yaml:"managements"`
}

// Management is one upstream management server.
type Management struct {
	Name string `yaml:"name"`
	// Address is the management gRPC URL, e.g. https://mgmt.example.com:443.
	Address string `yaml:"address"`
	// Token is the proxy access token issued by this management.
	Token string `yaml:"token"`
	// PeerURL is the management URL the proxy's embedded clients connect to
	// for accounts of this management. Defaults to Address.
	PeerURL string `yaml:"peer_url"`
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// LoadConfig reads a YAML config file. ${VAR} references are replaced with
// environment variables so tokens can be kept out of the file.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return ParseConfig(raw)
}

// ParseConfig parses and validates YAML config content.
func ParseConfig(raw []byte) (*Config, error) {
	expanded := envRef.ReplaceAllFunc(raw, func(m []byte) []byte {
		return []byte(os.Getenv(string(envRef.FindSubmatch(m)[1])))
	})

	cfg := &Config{}
	if err := yaml.Unmarshal(expanded, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:50051"
	}
	if c.InitialSyncTimeout <= 0 {
		c.InitialSyncTimeout = defaultInitialSyncTimeout
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("tls_cert_file and tls_key_file must be set together")
	}
	if len(c.Managements) == 0 {
		return errors.New("at least one management is required")
	}

	names := make(map[string]struct{}, len(c.Managements))
	for i := range c.Managements {
		m := &c.Managements[i]
		if m.Name == "" {
			return fmt.Errorf("managements[%d]: name is required", i)
		}
		if _, dup := names[m.Name]; dup {
			return fmt.Errorf("managements[%d]: duplicate name %q", i, m.Name)
		}
		names[m.Name] = struct{}{}
		if err := validateURL(m.Address); err != nil {
			return fmt.Errorf("management %q: address: %w", m.Name, err)
		}
		if m.Token == "" {
			return fmt.Errorf("management %q: token is required", m.Name)
		}
		if m.PeerURL == "" {
			m.PeerURL = m.Address
		} else if err := validateURL(m.PeerURL); err != nil {
			return fmt.Errorf("management %q: peer_url: %w", m.Name, err)
		}
	}
	return nil
}

func validateURL(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%q: scheme must be http or https", s)
	}
	if u.Host == "" {
		return fmt.Errorf("%q: host is required", s)
	}
	return nil
}
