# kvcache

kvcache implements a simple key/value datastore for a particular use case:

- Keys and values are just bytes
- Keys are small (on the order of 10 bytes) and values are large (kilobytes)
- Keys and values only need to be accessed for a fixed time window.
- Lookups are very much more frequent for more recent keys than for older keys

General implementation notes:

- Key/value pairs have a fixed expiration duration
- Key/value pairs are immutable, once written (until expired)
- Key/value pairs are stored on disk in a rotating set of fixed-size append-only logs
- The keys are associated with offsets into the log by an in-memory hashtable (can be reconstructed from the
  logs)
- Recent key/value pairs are duplicated in another in-memory hashtable (with the complete value) for fast
  lookup

## TODO

- chunk test
- db test
- write index files (or write index to beginning of file)
- reopen existing DB
- TODOs
- benchmark again, including with ~30GB of data, on a longer-running server
- run on demo server
