// Package nats exposes NATS server monitoring and read-only JetStream
// (streams, consumers, messages, KV and object stores) as a VGI catalog.
//
// To add a function: write it (see monitor.go, jetstream.go or scan.go), then
// register it in NewWorker below.
//
// Run it:
//
//	vgi-nats-worker                      # stdio: what DuckDB launches by default
//	vgi-nats-worker --http --port 8000   # HTTP: for Docker / hosted deploys
//	vgi-nats-worker --help
//
// Attach it from DuckDB:
//
//	ATTACH 'nats' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-nats-worker');
package nats

import (
	"github.com/Query-farm/vgi-go/vgi"
)

// allExamples lists every function's examples for the schema-level tag.
func allExamples() []exampleQuery {
	return exampleQueries(serverInfoExamples, connectionsExamples, routesExamples, gatewaysExamples, leafExamples,
		subsExamples, subStatsExamples, accountsExamples, accStatsExamples, jsServersExamples, jszStreamsExamples,
		jszConsumersExamples, healthExamples, accountInfoExamples, streamsExamples, subjectsExamples, consumersExamples,
		messagesExamples, kvBucketsExamples, kvEntriesExamples, objBucketsExamples, objectsExamples)
}

// NewWorker builds the worker: one catalog, one main schema. Extra options
// (logging, storage) are appended after the catalog options.
func NewWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	sourceURL, version := SourceURL, Version
	base := []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment(CatalogDocMD),
		vgi.WithCatalogTags(CatalogTags),
		vgi.WithCatalogInfo(vgi.CatalogInfo{SourceURL: &sourceURL, ImplementationVersion: &version}),
		vgi.WithSchemaComments(map[string]string{"main": SchemaDocMD}),
		vgi.WithSchemaTags(map[string]map[string]string{"main": schemaTags(allExamples())}),
		vgi.WithSecretTypes(APISecretType),
	}
	w := vgi.NewWorker(append(base, opts...)...)
	for _, f := range []vgi.TableFunction{
		newServerInfo(), newConnections(), newRoutes(), newGateways(), newLeafnodes(), newSubscriptions(),
		newSubscriptionStats(), newAccounts(), newAccountStats(), newJetStreamServers(), newJszStreams(),
		newJszConsumers(), newHealth(),
		newAccountInfo(), newStreams(), newStreamSubjects(), newConsumers(), newKVBuckets(), newObjectStores(), newObjects(),
	} {
		w.RegisterTable(f)
	}
	w.RegisterTable(vgi.AsTableFunction[ScanState](&MessagesFunc{}))
	w.RegisterTable(vgi.AsTableFunction[ScanState](&KVEntriesFunc{}))
	return w
}
