package upstash

// KV represents a Key-Value pair.
type KV struct {
	Key   string
	Value string
}

// SetOptions represents options for the SET command.
type SetOptions struct {
	// EX sets the specified expire time, in seconds.
	EX int

	// PX sets the specified expire time, in milliseconds.
	PX int

	// NX only sets the key if it does not already exist.
	NX bool

	// XX only sets the key if it already exists.
	XX bool
}

// GetEXOptions represents options for the GETEX command.
// Only one of these should be set.
type GetEXOptions struct {
	// EX sets the specified expire time, in seconds.
	EX int

	// PX sets the specified expire time, in milliseconds.
	PX int

	// EXAT sets the specified Unix time at which the key will expire, in seconds.
	EXAT int

	// PXAT sets the specified Unix time at which the key will expire, in milliseconds.
	PXAT int

	// PERSIST removes the time to live associated with the key.
	PERSIST bool
}

// ScanOptions represents options for the SCAN commands.
type ScanOptions struct {
	// Match filters keys by a pattern.
	Match string
	// Count provides a hint for the amount of work to do per iteration.
	Count int
	// Type filters keys by their Redis type (only for SCAN).
	Type string
}

// ScanResult represents the result of a SCAN command.
type ScanResult struct {
	Cursor string
	Items  []string
}

// GeoLocation represents a longitude and latitude pair.
type GeoLocation struct {
	Longitude float64
	Latitude  float64
	Member    string
}

// StreamMessage represents a single message in a stream.
type StreamMessage struct {
	ID     string
	Values map[string]string
}

// XReadGroupOptions represents options for the XREADGROUP command.
type XReadGroupOptions struct {
	Group    string
	Consumer string
	Count    int
	Block    int
	NoAck    bool
}
