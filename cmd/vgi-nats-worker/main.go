// Command vgi-nats-worker serves the nats catalog to DuckDB.
//
//	vgi-nats-worker                                  # stdio (what DuckDB launches)
//	vgi-nats-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-nats-worker --unix /tmp/nats.sock         # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'nats' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-nats-worker');
package main

import (
	"os"

	"github.com/Query-farm/vgi-go/vgi"

	"github.com/Query-farm/vgi-nats/internal/nats"
	"github.com/Query-farm/vgi-nats/internal/serve"
)

func main() {
	serve.Main("vgi-nats-worker", os.Args[1:], func() *vgi.Worker { return nats.NewWorker() })
}
