// Command natsfixture runs the deterministic NATS fixture the SQL tests read.
//
// It embeds nats-server (Apache-2.0) in-process, so neither CI nor a developer
// needs Docker: two servers start on random loopback ports and the fixture
// data is published before the ports are announced.
//
//   - main server "vgi-nats-fixture": JetStream on, HTTP monitoring on, two
//     accounts. SYS is the system account (user sys / sys-pass); APP has
//     JetStream (user app / app-pass, plus an NKey user with the seed in
//     FixtureNKeySeed).
//   - token server "vgi-nats-token": token authentication only, no JetStream,
//     to test TOKEN secrets and the $SYS-less monitoring error.
//
// Data in APP (published in this order, so stream sequences are fixed):
//
//	ORDERS  orders.>  file   10 messages (headers, a binary and an empty payload);
//	                         a time boundary is printed between seq 5 and 6
//	EVENTS  events.*, audit.>  memory  4 messages
//	BULK    bulk.<n%7>  file  25,000 messages (multi-batch scans)
//	WORK    jobs.*  work-queue  6 messages, durable consumer "worker" with nothing pulled
//	consumers on ORDERS: shipper (filter orders.*.shipped, 1 delivered+acked), audit (nothing delivered)
//	KV config (history 5): app.color red->blue->green, app.size 10, feature.x on then deleted
//	object store files: hello.txt, data.bin (300,000 bytes, 3 chunks), old.txt (deleted)
//
// Output on stdout, one KEY=value per line, then PORT:<client port> last:
//
//	NATS_URL, NATS_MON_URL, NATS_TOKEN_URL, NATS_TOKEN_MON_URL, NATS_BOUNDARY
//
// It runs until killed (SIGTERM/SIGINT) and removes its JetStream store.
package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	server "github.com/nats-io/nats-server/v2/server"
	natsio "github.com/nats-io/nats.go"
)

// FixtureNKeyPublic is the public key of the fixture's NKey user (seed
// SUAA5KW3YKG764K5R5FABZUV73YJ7CGKJEDOEYI55P3TUCKHWVETOI3KHQ, test-only).
const FixtureNKeyPublic = "UAW7PDGTWNHEKA4I5BJ4VHAK6JEKQOVW3BFWC5EYKIWLFUYW2XTPVTA5"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "natsfixture:", err)
		os.Exit(1)
	}
}

func startServer(conf string) (*server.Server, error) {
	dir, err := os.MkdirTemp("", "vgi-nats-fixture-*")
	if err != nil {
		return nil, err
	}
	conf = strings.ReplaceAll(conf, "STORE_DIR", filepath.ToSlash(filepath.Join(dir, "js")))
	path := filepath.Join(dir, "server.conf")
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		return nil, err
	}
	opts, err := server.ProcessConfigFile(path)
	if err != nil {
		return nil, err
	}
	opts.NoLog, opts.NoSigs = true, true
	s, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}
	go s.Start()
	if !s.ReadyForConnections(20 * time.Second) {
		return nil, fmt.Errorf("server %s did not start", opts.ServerName)
	}
	cleanups = append(cleanups, func() { s.Shutdown(); s.WaitForShutdown(); _ = os.RemoveAll(dir) })
	return s, nil
}

var cleanups []func()

const mainConf = `
server_name: vgi-nats-fixture
listen: 127.0.0.1:-1
http: 127.0.0.1:-1
jetstream { store_dir: "STORE_DIR", max_memory_store: 64MB, max_file_store: 512MB }
accounts {
  SYS { users: [ { user: sys, password: sys-pass } ] }
  APP {
    jetstream: enabled
    users: [ { user: app, password: app-pass }, { nkey: ` + FixtureNKeyPublic + ` } ]
  }
}
system_account: SYS
`

const tokenConf = `
server_name: vgi-nats-token
listen: 127.0.0.1:-1
http: 127.0.0.1:-1
authorization { token: "fixture-token" }
`

func run() error {
	defer func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}()
	mainSrv, err := startServer(mainConf)
	if err != nil {
		return err
	}
	tokenSrv, err := startServer(tokenConf)
	if err != nil {
		return err
	}
	nc, err := natsio.Connect(mainSrv.ClientURL(), natsio.UserInfo("app", "app-pass"), natsio.Name("fixture-publisher"))
	if err != nil {
		return err
	}
	defer nc.Close()
	boundary, err := publish(nc)
	if err != nil {
		return err
	}
	// A token-server client with a name, so connections() there is not empty.
	tc, err := natsio.Connect(tokenSrv.ClientURL(), natsio.Token("fixture-token"), natsio.Name("token-client"))
	if err != nil {
		return err
	}
	defer tc.Close()

	fmt.Printf("NATS_URL=%s\n", mainSrv.ClientURL())
	fmt.Printf("NATS_MON_URL=http://%s\n", mainSrv.MonitorAddr().String())
	fmt.Printf("NATS_TOKEN_URL=%s\n", tokenSrv.ClientURL())
	fmt.Printf("NATS_TOKEN_MON_URL=http://%s\n", tokenSrv.MonitorAddr().String())
	fmt.Printf("NATS_BOUNDARY=%s\n", boundary.UTC().Format("2006-01-02 15:04:05.000000+00"))
	fmt.Printf("PORT:%d\n", mainSrv.Addr().(*net.TCPAddr).Port)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	<-sig
	return nil
}
