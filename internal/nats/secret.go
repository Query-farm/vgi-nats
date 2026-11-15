// Connections: the `nats` DuckDB secret, the NATS client connection it
// describes, and the monitoring source (HTTP monitoring port or the $SYS
// request API).
//
// Credentials are read from the secret on every call and never stored in
// table-function state, which travels back to DuckDB over the HTTP transport.
// Nothing is read implicitly: no ~/.nats contexts, no nsc stores, no
// environment variables.

package nats

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	natsio "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

const (
	// SecretType is the DuckDB secret type every function reads.
	SecretType = "nats"
	// ConnectTimeout bounds the initial connection.
	ConnectTimeout = 10 * time.Second
	// RequestTimeout bounds every JetStream API or monitoring request.
	RequestTimeout = 30 * time.Second
)

// userAgent names the worker's connections (visible in connz) and HTTP requests.
const userAgent = "vgi-nats-worker/" + Version

func secretField(name, doc string, redact bool) arrow.Field {
	keys := []string{"vgi_doc"}
	vals := []string{doc}
	if redact {
		keys = append(keys, "redact")
		vals = append(vals, "true")
	}
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true,
		Metadata: arrow.NewMetadata(keys, vals)}
}

// APISecretType is registered with DuckDB so users can run
// CREATE SECRET (TYPE nats, SERVERS 'nats://host:4222', USER '...', PASSWORD '...').
var APISecretType = vgi.SecretTypeSpec{
	Name: SecretType,
	Description: "Connection to a NATS server or cluster: server URLs, one authentication method " +
		"(token, user/password, NKey seed or a .creds file's contents), optional TLS, and the HTTP monitoring URL.",
	Schema: arrow.NewSchema([]arrow.Field{
		secretField("servers", "Comma-separated server URLs, e.g. 'nats://n1:4222,nats://n2:4222' (tls:// for TLS). Default nats://127.0.0.1:4222.", false),
		secretField("token", "Authentication token.", true),
		secretField("user", "User name for user/password authentication.", false),
		secretField("password", "Password for user/password authentication.", true),
		secretField("nkey_seed", "NKey user seed (starts with SU), for NKey authentication.", true),
		secretField("creds", "Contents of a decentralized-auth .creds file (user JWT plus NKey seed).", true),
		secretField("tls_ca", "PEM CA certificate(s) used to verify the server.", false),
		secretField("tls_cert", "PEM client certificate for mutual TLS.", false),
		secretField("tls_key", "PEM private key of the client certificate.", true),
		secretField("monitoring_url", "Comma-separated HTTP monitoring base URLs (port 8222), one per server, e.g. 'http://n1:8222'. "+
			"Without it, monitoring functions use the $SYS request API, which needs system-account credentials.", false),
		secretField("jetstream_domain", "JetStream domain to address (leaf-node and multi-domain deployments).", false),
	}, &secretSchemaMetadata),
}

var secretSchemaMetadata = arrow.NewMetadata(
	[]string{"vgi_doc_url"},
	[]string{SourceURL + "#secrets-and-configuration"})

// secretRequirements is declared by every server function.
var secretRequirements = []vgi.SecretRequirement{{SecretType: SecretType}}

// secretNamed is implemented by every argument struct.
type secretNamed interface{ secret() string }

// bindNamedSecret binds a schema. When secret_name names a secret, the first
// bind asks the extension to resolve that secret by name (two-phase bind).
func bindNamedSecret(params *vgi.BindParams, name string, schema *arrow.Schema) (*vgi.BindResponse, error) {
	if name != "" && !params.ResolvedSecretsProvided {
		return &vgi.BindResponse{SecretScopeRequest: []vgi.SecretLookup{{SecretType: SecretType, SecretName: name}}}, nil
	}
	return vgi.BindSchema(schema)
}

// config is a resolved secret.
type config struct {
	Servers       string
	Token         string
	User          string
	Password      string
	NKeySeed      string
	Creds         string
	TLSCA         string
	TLSCert       string
	TLSKey        string
	MonitoringURL []string
	Domain        string
}

// key identifies a connection configuration (a hash: no secret is kept as a map key).
func (c *config) key() string {
	h := sha256.New()
	for _, s := range []string{c.Servers, c.Token, c.User, c.Password, c.NKeySeed, c.Creds, c.TLSCA, c.TLSCert, c.TLSKey, c.Domain} {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// resolveConfig picks the secret (the named one when name is non-empty).
func resolveConfig(secrets vgi.Secrets, name string) (*config, error) {
	var fields map[string]interface{}
	var ok bool
	if name != "" {
		fields, ok = secrets.LookupNamed(SecretType, name)
		if !ok {
			return nil, fmt.Errorf("no nats secret named %q: create it with CREATE SECRET %s (TYPE nats, SERVERS 'nats://host:4222', ...)", name, name)
		}
	} else {
		fields, ok = secrets.Lookup(SecretType)
		if !ok {
			return nil, errors.New("no nats secret found: create one with CREATE SECRET (TYPE nats, SERVERS 'nats://127.0.0.1:4222') " +
				"and add TOKEN, USER/PASSWORD, NKEY_SEED or CREDS as your server needs")
		}
	}
	get := func(k string) string { return strings.TrimSpace(vgi.RenderSecretValue(fields[k])) }
	c := &config{
		Servers: get("servers"), Token: get("token"), User: get("user"), Password: vgi.RenderSecretValue(fields["password"]),
		NKeySeed: get("nkey_seed"), Creds: get("creds"), TLSCA: get("tls_ca"), TLSCert: get("tls_cert"), TLSKey: get("tls_key"),
		Domain: get("jetstream_domain"),
	}
	if c.Servers == "" {
		c.Servers = natsio.DefaultURL
	}
	for _, s := range strings.Split(c.Servers, ",") {
		u, err := url.Parse(strings.TrimSpace(s))
		if err != nil || u.Host == "" || !validScheme(u.Scheme, "nats", "tls", "ws", "wss") {
			return nil, fmt.Errorf("the nats secret's SERVERS must be nats://, tls://, ws:// or wss:// URLs, got %q", s)
		}
	}
	for _, m := range strings.Split(get("monitoring_url"), ",") {
		m = strings.TrimRight(strings.TrimSpace(m), "/")
		if m == "" {
			continue
		}
		u, err := url.Parse(m)
		if err != nil || u.Host == "" || !validScheme(u.Scheme, "http", "https") {
			return nil, fmt.Errorf("the nats secret's MONITORING_URL must be http(s) URLs, got %q", m)
		}
		c.MonitoringURL = append(c.MonitoringURL, m)
	}
	methods := 0
	for _, s := range []string{c.Token, c.User, c.NKeySeed, c.Creds} {
		if s != "" {
			methods++
		}
	}
	if methods > 1 {
		return nil, errors.New("the nats secret sets more than one of TOKEN, USER, NKEY_SEED and CREDS: use exactly one authentication method")
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return nil, errors.New("the nats secret needs both TLS_CERT and TLS_KEY for mutual TLS")
	}
	return c, nil
}

func validScheme(s string, allowed ...string) bool {
	for _, a := range allowed {
		if strings.EqualFold(s, a) {
			return true
		}
	}
	return false
}

// options turns the config into nats.go connect options.
func (c *config) options() ([]natsio.Option, error) {
	opts := []natsio.Option{
		natsio.Name(userAgent), natsio.Timeout(ConnectTimeout), natsio.MaxReconnects(3),
		natsio.NoCallbacksAfterClientClose(),
	}
	switch {
	case c.Token != "":
		opts = append(opts, natsio.Token(c.Token))
	case c.User != "":
		opts = append(opts, natsio.UserInfo(c.User, c.Password))
	case c.NKeySeed != "":
		kp, err := nkeys.FromSeed([]byte(c.NKeySeed))
		if err != nil {
			return nil, errors.New("the nats secret's NKEY_SEED is not a valid NKey seed")
		}
		pub, err := kp.PublicKey()
		if err != nil {
			return nil, err
		}
		opts = append(opts, natsio.Nkey(pub, func(nonce []byte) ([]byte, error) { return kp.Sign(nonce) }))
	case c.Creds != "":
		jwt, err := nkeys.ParseDecoratedJWT([]byte(c.Creds))
		if err != nil || jwt == "" {
			return nil, errors.New("the nats secret's CREDS does not contain a user JWT (paste the whole .creds file)")
		}
		kp, err := nkeys.ParseDecoratedNKey([]byte(c.Creds))
		if err != nil {
			return nil, errors.New("the nats secret's CREDS does not contain an NKey seed (paste the whole .creds file)")
		}
		seed, err := kp.Seed()
		if err != nil {
			return nil, errors.New("the nats secret's CREDS does not contain an NKey seed")
		}
		opts = append(opts, natsio.UserJWTAndSeed(jwt, string(seed)))
	}
	if c.TLSCA != "" || c.TLSCert != "" {
		tc, err := c.tlsConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, natsio.Secure(tc))
	}
	return opts, nil
}

func (c *config) tlsConfig() (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.TLSCA != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(c.TLSCA)) {
			return nil, errors.New("the nats secret's TLS_CA holds no PEM certificate")
		}
		tc.RootCAs = pool
	}
	if c.TLSCert != "" {
		cert, err := tls.X509KeyPair([]byte(c.TLSCert), []byte(c.TLSKey))
		if err != nil {
			return nil, fmt.Errorf("the nats secret's TLS_CERT/TLS_KEY: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

// pool keeps one connection per configuration for the life of the process,
// so consecutive batches of a scan do not reconnect.
var pool = struct {
	sync.Mutex
	conns map[string]*natsio.Conn
}{conns: map[string]*natsio.Conn{}}

// connect returns a live connection for the configuration.
func (c *config) connect() (*natsio.Conn, error) {
	k := c.key()
	pool.Lock()
	defer pool.Unlock()
	if nc, ok := pool.conns[k]; ok && !nc.IsClosed() {
		return nc, nil
	}
	opts, err := c.options()
	if err != nil {
		return nil, err
	}
	nc, err := natsio.Connect(c.Servers, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS at %s: %w", redactURLs(c.Servers), err)
	}
	pool.conns[k] = nc
	return nc, nil
}

// redactURLs drops any user:password@ from server URLs for error messages.
func redactURLs(servers string) string {
	parts := strings.Split(servers, ",")
	for i, p := range parts {
		if u, err := url.Parse(strings.TrimSpace(p)); err == nil && u.User != nil {
			u.User = nil
			parts[i] = u.String()
		}
	}
	return strings.Join(parts, ",")
}

// jetStream returns a JetStream context for the configuration.
func (c *config) jetStream() (jetstream.JetStream, error) {
	nc, err := c.connect()
	if err != nil {
		return nil, err
	}
	opts := []jetstream.JetStreamOpt{jetstream.WithDefaultTimeout(RequestTimeout)}
	if c.Domain != "" {
		return jetstream.NewWithDomain(nc, c.Domain, opts...)
	}
	return jetstream.New(nc, opts...)
}

// jsError rewrites common JetStream failures into actionable messages.
func jsError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, jetstream.ErrJetStreamNotEnabled), errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount):
		return fmt.Errorf("JetStream is not enabled for this account: %w", err)
	case errors.Is(err, natsio.ErrNoResponders):
		return fmt.Errorf("no JetStream API responded (is JetStream enabled for this account, and is JETSTREAM_DOMAIN right?): %w", err)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, natsio.ErrTimeout):
		return fmt.Errorf("JetStream request timed out after %s: %w", RequestTimeout, err)
	}
	return err
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
