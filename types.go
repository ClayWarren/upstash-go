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
