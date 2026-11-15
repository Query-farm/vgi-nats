// The monitoring source: either the HTTP monitoring endpoints (MONITORING_URL,
// one base URL per server) or the $SYS request API over the NATS connection,
// which needs system-account credentials and answers once per server.

package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	natsio "github.com/nats-io/nats.go"
)

// monReply is one server's answer to a monitoring request.
type monReply struct {
	ServerID   string
	ServerName string
	Data       json.RawMessage
}

// monRequest names one monitoring endpoint in both forms.
type monRequest struct {
	// Path is the HTTP endpoint, e.g. "connz".
	Path string
	// Query holds the HTTP query parameters.
	Query url.Values
	// SysSubject is the $SYS request subject, e.g. "$SYS.REQ.SERVER.PING.CONNZ".
	SysSubject string
	// SysBody is the request options for the $SYS form (JSON-encoded); nil sends none.
	SysBody any
}

// SysGather bounds how long a $SYS ping waits for every server to answer.
var SysGather = 2 * time.Second

// SysQuiet ends a $SYS ping early once no server has answered for this long.
var SysQuiet = 300 * time.Millisecond

var httpClient = &http.Client{Timeout: RequestTimeout}

// monitor runs a monitoring request against every server the secret names.
func monitor(ctx context.Context, c *config, r monRequest) ([]monReply, error) {
	if len(c.MonitoringURL) > 0 {
		out := make([]monReply, 0, len(c.MonitoringURL))
		for _, base := range c.MonitoringURL {
			body, err := httpGet(ctx, base, r.Path, r.Query)
			if err != nil {
				return nil, err
			}
			rep := monReply{Data: body}
			var ids struct {
				ServerID   string `json:"server_id"`
				ServerName string `json:"server_name"`
			}
			_ = json.Unmarshal(body, &ids)
			rep.ServerID, rep.ServerName = ids.ServerID, ids.ServerName
			if rep.ServerName == "" && r.Path != "varz" {
				// connz, jsz, healthz, ... carry no server name: take it from varz.
				if vb, err := httpGet(ctx, base, "varz", nil); err == nil {
					_ = json.Unmarshal(vb, &ids)
					rep.ServerName = ids.ServerName
					if rep.ServerID == "" {
						rep.ServerID = ids.ServerID
					}
				}
			}
			out = append(out, rep)
		}
		return out, nil
	}
	return sysPing(ctx, c, r)
}

func httpGet(ctx context.Context, base, path string, q url.Values) (json.RawMessage, error) {
	target := base + "/" + path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "application/json")
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("nats monitoring %s/%s: %w", base, path, err)
			if errors.Is(err, context.Canceled) {
				return nil, lastErr
			}
			time.Sleep(time.Duration(200*attempt) * time.Millisecond)
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<30))
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode >= 500 && path != "healthz") {
			lastErr = fmt.Errorf("nats monitoring %s/%s: HTTP %d: %s", base, path, resp.StatusCode, snippet(body))
			wait := time.Duration(250<<attempt) * time.Millisecond
			if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && s >= 0 {
				wait = min(time.Duration(s)*time.Second, 30*time.Second)
			}
			time.Sleep(wait)
			continue
		}
		if readErr != nil {
			return nil, fmt.Errorf("nats monitoring %s/%s: reading response: %w", base, path, readErr)
		}
		// /healthz answers 503 with a JSON status when unhealthy: that is data.
		if resp.StatusCode/100 != 2 && (path != "healthz" || !json.Valid(body)) {
			msg := fmt.Sprintf("nats monitoring %s/%s: HTTP %d: %s", base, path, resp.StatusCode, snippet(body))
			if resp.StatusCode == http.StatusNotFound {
				msg += " (this server does not serve the endpoint; is MONITORING_URL the HTTP monitoring port, usually 8222?)"
			}
			return nil, errors.New(msg)
		}
		return body, nil
	}
	return nil, lastErr
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}

// sysEnvelope is the $SYS reply wrapper.
type sysEnvelope struct {
	Server struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"server"`
	Data  json.RawMessage `json:"data"`
	Error *struct {
		Code        int    `json:"code"`
		Description string `json:"description"`
	} `json:"error"`
}

// errNoSys explains how to enable monitoring.
var errNoSys = errors.New("no server answered the $SYS monitoring request: set MONITORING_URL in the nats secret to the " +
	"HTTP monitoring port (e.g. 'http://host:8222'), or connect with system-account credentials " +
	"(a user of the account named system_account in the server config)")

func sysPing(ctx context.Context, c *config, r monRequest) ([]monReply, error) {
	nc, err := c.connect()
	if err != nil {
		return nil, err
	}
	var body []byte
	if r.SysBody != nil {
		if body, err = json.Marshal(r.SysBody); err != nil {
			return nil, err
		}
	}
	inbox := nc.NewRespInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.PublishMsg(&natsio.Msg{Subject: r.SysSubject, Reply: inbox, Data: body}); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(SysGather)
	var out []monReply
	for {
		wait := time.Until(deadline)
		if len(out) > 0 {
			wait = min(wait, SysQuiet)
		}
		if wait <= 0 {
			break
		}
		msg, err := sub.NextMsgWithContext(withTimeout(ctx, wait))
		if err != nil {
			if errors.Is(err, natsio.ErrNoResponders) {
				return nil, errNoSys
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, natsio.ErrTimeout) {
				break
			}
			return nil, err
		}
		if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
			return nil, errNoSys
		}
		var env sysEnvelope
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			slog.Warn("nats: undecodable $SYS reply", "subject", r.SysSubject, "error", err)
			continue
		}
		if env.Error != nil {
			return nil, fmt.Errorf("nats %s: server %s: %s (code %d)", r.SysSubject, env.Server.Name, env.Error.Description, env.Error.Code)
		}
		out = append(out, monReply{ServerID: env.Server.ID, ServerName: env.Server.Name, Data: env.Data})
	}
	if len(out) == 0 {
		return nil, errNoSys
	}
	return out, nil
}

// withTimeout derives a context that the caller does not need to cancel
// (it ends with the parent or the timeout, whichever is first).
func withTimeout(ctx context.Context, d time.Duration) context.Context {
	c, cancel := context.WithTimeout(ctx, d)
	time.AfterFunc(d+time.Second, cancel)
	return c
}

// parseRTT reads a Go duration string ("122µs", "1.5ms"); NULL when absent.
func parseRTT(s string) any {
	if s == "" {
		return nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return nil
	}
	return d.Seconds()
}

// subjectMatches reports whether a subscription subject (which may hold * and
// > wildcards) receives messages published on subject. When subject itself
// has wildcards, a subscription matches if it is identical or its pattern
// covers every token of subject.
func subjectMatches(pattern, subject string) bool {
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i
		}
		if i >= len(s) {
			return false
		}
		if tok == "*" {
			if s[i] == ">" {
				return false
			}
			continue
		}
		if tok != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}
