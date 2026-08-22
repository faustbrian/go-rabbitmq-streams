package rabbitstream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	// MaxEndpoints bounds connection-rotation state and diagnostic output.
	MaxEndpoints = 16
	// MaxReconnectAttempts is the largest accepted finite reconnect budget.
	MaxReconnectAttempts = 32
	maxEndpointHostBytes = 255
)

const (
	defaultConnectTimeout       = 10 * time.Second
	defaultRPCTimeout           = 10 * time.Second
	defaultHeartbeat            = 30 * time.Second
	minimumHeartbeat            = 3 * time.Second
	defaultReconnectAttempts    = 8
	defaultInitialReconnectWait = 100 * time.Millisecond
	defaultMaxReconnectWait     = 30 * time.Second
)

// Endpoint identifies one RabbitMQ Streams listener. Credentials are kept out
// of endpoint values so they cannot be exposed by URI formatting.
type Endpoint struct {
	// Host is a DNS name or IP literal without credentials or a scheme.
	Host string
	// Port is the RabbitMQ Streams listener port.
	Port uint16
}

// Credentials are an owned authentication snapshot. A provider must return a
// fresh password slice on every call so credential rotation can occur between
// connection attempts.
type Credentials struct {
	// Username is the RabbitMQ authentication identity.
	Username string
	// Password is an owned secret snapshot that callers must not log.
	Password []byte
}

// CredentialProvider supplies a fresh credential snapshot for each connection
// attempt. Implementations must honor cancellation and must not render secrets.
type CredentialProvider interface {
	// Credentials returns a fresh owned snapshot and honors ctx cancellation.
	Credentials(context.Context) (Credentials, error)
}

type staticCredentialProvider struct {
	username string
	password []byte
}

// StaticCredentials copies password immediately and returns a provider that
// returns owned copies. Applications requiring rotation should implement
// CredentialProvider instead.
func StaticCredentials(username string, password []byte) StringerCredentialProvider {
	return &staticCredentialProvider{
		username: username,
		password: append([]byte(nil), password...),
	}
}

// StringerCredentialProvider combines the provider contract with a safe
// diagnostic identity. It exists to make the safety of built-in providers
// explicit without requiring custom providers to implement String.
type StringerCredentialProvider interface {
	CredentialProvider
	fmt.Stringer
}

// Credentials returns a fresh owned password copy and honors cancellation.
func (provider *staticCredentialProvider) Credentials(ctx context.Context) (Credentials, error) {
	if ctx == nil {
		return Credentials{}, invalidConfiguration(errors.New("credential context is nil"))
	}
	if err := ctx.Err(); err != nil {
		return Credentials{}, err
	}
	return Credentials{
		Username: provider.username,
		Password: append([]byte(nil), provider.password...),
	}, nil
}

// String returns a credential identity that never includes the password.
func (provider *staticCredentialProvider) String() string {
	return "static credentials for " + provider.username
}

// SecurityMode selects verified TLS or an explicit local-development-only
// plaintext connection.
type SecurityMode uint8

const (
	// SecurityTLS requires certificate-verified TLS and is the zero-value mode.
	SecurityTLS SecurityMode = iota
	// SecurityPlaintext is accepted only through DevelopmentPlaintextSecurity.
	SecurityPlaintext
)

// SecurityConfig owns transport security policy. The zero value is verified
// TLS with a TLS 1.2 minimum. InsecureSkipVerify is always rejected.
type SecurityConfig struct {
	// Mode selects verified TLS or explicit development-only plaintext.
	Mode SecurityMode
	// TLS is cloned during normalization and must retain peer verification.
	TLS *tls.Config

	developmentPlaintext bool
}

// DevelopmentPlaintextSecurity opts into unencrypted local development. It
// must never be used for production credentials or traffic.
func DevelopmentPlaintextSecurity() SecurityConfig {
	return SecurityConfig{
		Mode:                 SecurityPlaintext,
		developmentPlaintext: true,
	}
}

// ConnectionConfig defines finite connection, RPC, heartbeat, and reconnect
// budgets. Endpoint order is the caller's preferred rotation order.
type ConnectionConfig struct {
	// Endpoints is the ordered, finite broker rotation set.
	Endpoints []Endpoint
	// VirtualHost selects the RabbitMQ virtual host; empty selects the client default.
	VirtualHost string
	// Credentials is resolved again for each connection attempt.
	Credentials CredentialProvider
	// Security defines verified transport policy.
	Security SecurityConfig
	// ConnectTimeout bounds complete session establishment, including retries.
	ConnectTimeout time.Duration
	// RPCTimeout bounds one RabbitMQ Streams RPC attempt.
	RPCTimeout time.Duration
	// Heartbeat configures the negotiated connection heartbeat.
	Heartbeat time.Duration
	// MaxReconnectAttempts bounds endpoint attempts within one connection budget.
	MaxReconnectAttempts int
	// InitialReconnectDelay is the first delay between connection attempts.
	InitialReconnectDelay time.Duration
	// MaxReconnectBackoff caps exponential connection backoff.
	MaxReconnectBackoff time.Duration
	// Observer receives bounded best-effort lifecycle signals.
	Observer Observer
}

// Validate rejects unsafe or unbounded connection policy.
func (config ConnectionConfig) Validate() error {
	_, err := config.normalized()
	return err
}

// Normalized validates the policy and returns an owned configuration with all
// finite defaults applied. Credential resolution remains deferred.
func (config ConnectionConfig) Normalized() (ConnectionConfig, error) {
	return config.normalized()
}

func (config ConnectionConfig) normalized() (ConnectionConfig, error) {
	if len(config.Endpoints) == 0 || len(config.Endpoints) > MaxEndpoints {
		return ConnectionConfig{}, invalidConfiguration(errors.New("endpoint count is outside bounds"))
	}
	config.Endpoints = append([]Endpoint(nil), config.Endpoints...)
	for _, endpoint := range config.Endpoints {
		if invalidIdentifier(endpoint.Host, maxEndpointHostBytes) || endpoint.Port == 0 {
			return ConnectionConfig{}, invalidConfiguration(errors.New("endpoint is invalid"))
		}
	}
	if config.Credentials == nil {
		return ConnectionConfig{}, invalidConfiguration(errors.New("credential provider is required"))
	}
	if config.VirtualHost != "" && invalidIdentifier(config.VirtualHost, 255) {
		return ConnectionConfig{}, invalidConfiguration(errors.New("virtual host is invalid"))
	}

	if config.ConnectTimeout < 0 || config.RPCTimeout < 0 || config.Heartbeat < 0 ||
		config.InitialReconnectDelay < 0 || config.MaxReconnectBackoff < 0 ||
		config.MaxReconnectAttempts < 0 {
		return ConnectionConfig{}, invalidConfiguration(errors.New("duration or retry count is negative"))
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = defaultConnectTimeout
	}
	if config.RPCTimeout == 0 {
		config.RPCTimeout = defaultRPCTimeout
	}
	if config.Heartbeat == 0 {
		config.Heartbeat = defaultHeartbeat
	}
	if config.MaxReconnectAttempts == 0 {
		config.MaxReconnectAttempts = defaultReconnectAttempts
	}
	if config.InitialReconnectDelay == 0 {
		config.InitialReconnectDelay = defaultInitialReconnectWait
	}
	if config.MaxReconnectBackoff == 0 {
		config.MaxReconnectBackoff = defaultMaxReconnectWait
	}
	if config.MaxReconnectAttempts > MaxReconnectAttempts ||
		config.InitialReconnectDelay > config.MaxReconnectBackoff ||
		config.Heartbeat < minimumHeartbeat {
		return ConnectionConfig{}, invalidConfiguration(errors.New("reconnect policy is outside bounds"))
	}

	if config.Security.Mode > SecurityPlaintext {
		return ConnectionConfig{}, invalidConfiguration(errors.New("security mode is invalid"))
	}
	if config.Security.Mode == SecurityPlaintext {
		if !config.Security.developmentPlaintext {
			return ConnectionConfig{}, invalidConfiguration(errors.New("plaintext requires development policy"))
		}
		return config, nil
	}
	if config.Security.TLS == nil {
		config.Security.TLS = &tls.Config{}
	} else {
		config.Security.TLS = config.Security.TLS.Clone()
	}
	if config.Security.TLS.InsecureSkipVerify {
		return ConnectionConfig{}, invalidConfiguration(errors.New("TLS verification cannot be disabled"))
	}
	if config.Security.TLS.MinVersion == 0 {
		config.Security.TLS.MinVersion = tls.VersionTLS12
	}
	if config.Security.TLS.MinVersion < tls.VersionTLS12 {
		return ConnectionConfig{}, invalidConfiguration(errors.New("TLS minimum is below 1.2"))
	}
	return config, nil
}

func invalidIdentifier(value string, maximum int) bool {
	if value == "" || len(value) > maximum || strings.TrimSpace(value) != value {
		return true
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}
