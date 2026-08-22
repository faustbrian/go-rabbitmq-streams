package rabbitstream

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"
)

func TestConnectionConfigDefaultsToVerifiedTLSAndBoundedOperation(t *testing.T) {
	t.Parallel()

	config := ConnectionConfig{
		Endpoints:   []Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
		Credentials: StaticCredentials("track", []byte("secret")),
	}

	normalized, err := config.normalized()
	if err != nil {
		t.Fatalf("normalized() error = %v", err)
	}
	if normalized.Security.Mode != SecurityTLS {
		t.Fatalf("security mode = %v, want TLS", normalized.Security.Mode)
	}
	if normalized.Security.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatalf("minimum TLS version = %x, want TLS 1.2", normalized.Security.TLS.MinVersion)
	}
	if normalized.Security.TLS.InsecureSkipVerify {
		t.Fatal("TLS verification was disabled")
	}
	if normalized.ConnectTimeout != 10*time.Second || normalized.RPCTimeout != 10*time.Second ||
		normalized.Heartbeat != 30*time.Second || normalized.MaxReconnectAttempts != 8 ||
		normalized.InitialReconnectDelay != 100*time.Millisecond ||
		normalized.MaxReconnectBackoff != 30*time.Second {
		t.Fatalf("defaults = %#v", normalized)
	}
}

func TestConnectionConfigAcceptsEveryExactUpperAndLowerBoundary(t *testing.T) {
	t.Parallel()

	endpoints := make([]Endpoint, MaxEndpoints)
	for index := range endpoints {
		endpoints[index] = Endpoint{Host: "rabbit mq", Port: 5551}
	}
	config := ConnectionConfig{
		Endpoints:             endpoints,
		Credentials:           StaticCredentials("track", []byte("secret")),
		Heartbeat:             minimumHeartbeat,
		MaxReconnectAttempts:  MaxReconnectAttempts,
		InitialReconnectDelay: time.Second,
		MaxReconnectBackoff:   time.Second,
	}
	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() exact boundaries error = %v", err)
	}

	config.Endpoints[0].Host = repeatByte('h', maxEndpointHostBytes)
	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() maximum host error = %v", err)
	}
}

func TestPlaintextRequiresExplicitDevelopmentPolicy(t *testing.T) {
	t.Parallel()

	base := ConnectionConfig{
		Endpoints:   []Endpoint{{Host: "localhost", Port: 5552}},
		Credentials: StaticCredentials("guest", []byte("guest")),
	}

	production := base
	production.Security = SecurityConfig{Mode: SecurityPlaintext}
	if err := production.Validate(); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("Validate() error = %v, want ErrInvalidConfiguration", err)
	}

	development := base
	development.Security = DevelopmentPlaintextSecurity()
	if err := development.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestCredentialsAreResolvedAsOwnedCopies(t *testing.T) {
	t.Parallel()

	password := []byte("secret")
	provider := StaticCredentials("track", password)
	password[0] = 'X'

	first, err := provider.Credentials(context.Background())
	if err != nil {
		t.Fatalf("Credentials() error = %v", err)
	}
	first.Password[0] = 'Y'
	second, err := provider.Credentials(context.Background())
	if err != nil {
		t.Fatalf("Credentials() second error = %v", err)
	}
	if got := string(second.Password); got != "secret" {
		t.Fatalf("password = %q, want owned original", got)
	}
	if got := provider.String(); got != "static credentials for track" {
		t.Fatalf("String() = %q", got)
	}
}

func TestConnectionConfigRejectsUnboundedOrUnsafeInput(t *testing.T) {
	t.Parallel()

	valid := ConnectionConfig{
		Endpoints:   []Endpoint{{Host: "rabbitmq.internal", Port: 5551}},
		Credentials: StaticCredentials("track", []byte("secret")),
	}

	tests := map[string]func(*ConnectionConfig){
		"no endpoints": func(config *ConnectionConfig) { config.Endpoints = nil },
		"too many endpoints": func(config *ConnectionConfig) {
			config.Endpoints = make([]Endpoint, MaxEndpoints+1)
		},
		"endpoint newline":               func(config *ConnectionConfig) { config.Endpoints[0].Host = "broker\nsecret" },
		"missing port":                   func(config *ConnectionConfig) { config.Endpoints[0].Port = 0 },
		"missing credentials":            func(config *ConnectionConfig) { config.Credentials = nil },
		"negative connect timeout":       func(config *ConnectionConfig) { config.ConnectTimeout = -time.Second },
		"heartbeat below client minimum": func(config *ConnectionConfig) { config.Heartbeat = 2 * time.Second },
		"unbounded reconnect attempts":   func(config *ConnectionConfig) { config.MaxReconnectAttempts = MaxReconnectAttempts + 1 },
		"insecure TLS": func(config *ConnectionConfig) {
			config.Security.TLS = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // verifies rejection
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config := valid
			config.Endpoints = append([]Endpoint(nil), valid.Endpoints...)
			mutate(&config)
			if err := config.Validate(); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("Validate() error = %v, want ErrInvalidConfiguration", err)
			}
		})
	}
}
