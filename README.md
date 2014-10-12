# kvcache

kvcache implements a simple key/value datastore for a particular use case:

- Keys and values are just bytes
- Keys are small (10-50 bytes) and values are large (kilobytes)
- Keys and values only need to be accessed for a fixed time window
- Lookups are very much more frequent for more recent keys than for older keys

General implementation notes:

- Key/value pairs have a fixed expiration duration
- Key/value pairs are immutable, once written (until expired)
- Key/value pairs are stored on disk in a rotating set of fixed-size append-only logs
- The keys are associated with offsets into the log by an in-memory hashtable (can be reconstructed from an
  index written alongside each log)
