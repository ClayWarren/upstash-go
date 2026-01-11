package upstash

import (
	"context"
	"fmt"

	"github.com/claywarren/upstash-go/internal/rest"
)

// Keys returns all keys matching the provided pattern.
func (u *Upstash) Keys(ctx context.Context, pattern string) ([]string, error) {
	res, err := u.client.Read(ctx, rest.Request{
		Path: []string{"keys", pattern},
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return []string{}, nil
	}

	// Handle conversion from []interface{} (which JSON decoder produces) to []string
	if list, ok := res.([]any); ok {
		keys := make([]string, len(list))
		for i, v := range list {
			keys[i] = fmt.Sprint(v)
		}
		return keys, nil
	}

	// Fallback if it's already []string (e.g. from a different client implementation or mock)
	if list, ok := res.([]string); ok {
		return list, nil
	}

	return nil, fmt.Errorf("unexpected return type for keys: %T", res)
}

// Del removes the specified keys. A key is ignored if it does not exist.
func (u *Upstash) Del(ctx context.Context, keys ...string) (int, error) {
	args := make([]any, 0, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "DEL", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Exists returns if key exists.
func (u *Upstash) Exists(ctx context.Context, keys ...string) (int, error) {
	args := make([]any, 0, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "EXISTS", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Expire sets a timeout on key.
func (u *Upstash) Expire(ctx context.Context, key string, seconds int) (int, error) {
	res, err := u.Send(ctx, "EXPIRE", key, seconds)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Ttl returns the remaining time to live of a key that has a timeout.
func (u *Upstash) Ttl(ctx context.Context, key string) (int, error) {
	res, err := u.Send(ctx, "TTL", key)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// FlushAll deletes all keys of all existing databases.
func (u *Upstash) FlushAll(ctx context.Context) error {
	_, err := u.client.Write(ctx, rest.Request{
		Body: []string{"flushall"},
	})
	return err
}

// Scan iterates over the keys in the database.
func (u *Upstash) Scan(ctx context.Context, cursor string, options ScanOptions) (ScanResult, error) {
	return u.scan(ctx, "", cursor, options, "SCAN")
}

// Copy copies the value stored at the source key to the destination key.
func (u *Upstash) Copy(ctx context.Context, source, destination string) (int, error) {
	res, err := u.Send(ctx, "COPY", source, destination)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Dump returns a serialized version of the value stored at the specified key.
func (u *Upstash) Dump(ctx context.Context, key string) (string, error) {
	res, err := u.Send(ctx, "DUMP", key)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return res.(string), nil
}

// ExpireAt sets an expiration time for a key using a Unix timestamp.
func (u *Upstash) ExpireAt(ctx context.Context, key string, timestamp int64) (int, error) {
	res, err := u.Send(ctx, "EXPIREAT", key, timestamp)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Persist removes the expiration from a key.
func (u *Upstash) Persist(ctx context.Context, key string) (int, error) {
	res, err := u.Send(ctx, "PERSIST", key)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// PExpire sets a timeout on key in milliseconds.
func (u *Upstash) PExpire(ctx context.Context, key string, milliseconds int64) (int, error) {
	res, err := u.Send(ctx, "PEXPIRE", key, milliseconds)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// PTtl returns the remaining time to live of a key that has a timeout in milliseconds.
func (u *Upstash) PTtl(ctx context.Context, key string) (int64, error) {
	res, err := u.Send(ctx, "PTTL", key)
	if err != nil {
		return 0, err
	}
	return int64(res.(float64)), nil
}

// RandomKey returns a random key from the currently selected database.
func (u *Upstash) RandomKey(ctx context.Context) (string, error) {
	res, err := u.Send(ctx, "RANDOMKEY")
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return res.(string), nil
}

// Rename renames key to newkey.
func (u *Upstash) Rename(ctx context.Context, key, newkey string) error {
	_, err := u.Send(ctx, "RENAME", key, newkey)
	return err
}

// RenameNX renames key to newkey if the new key does not yet exist.
func (u *Upstash) RenameNX(ctx context.Context, key, newkey string) (int, error) {
	res, err := u.Send(ctx, "RENAMENX", key, newkey)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Touch alters the last access time of a key(s).
func (u *Upstash) Touch(ctx context.Context, keys ...string) (int, error) {
	args := make([]any, 0, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "TOUCH", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Type returns the string representation of the type of the value stored at key.
func (u *Upstash) Type(ctx context.Context, key string) (string, error) {
	res, err := u.Send(ctx, "TYPE", key)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// Unlink removes the specified keys. A key is ignored if it does not exist.
func (u *Upstash) Unlink(ctx context.Context, keys ...string) (int, error) {
	args := make([]any, 0, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "UNLINK", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Migrate atomically transfers a key from a Redis instance to another one.
func (u *Upstash) Migrate(ctx context.Context, host, port, key, destinationDB string, timeout int64, copy, replace bool, keys ...string) (string, error) {
	args := make([]any, 0, 6)
	args = append(args, host, port, key, destinationDB, timeout)
	if copy {
		args = append(args, "COPY")
	}
	if replace {
		args = append(args, "REPLACE")
	}
	if len(keys) > 0 {
		args = append(args, "KEYS")
		for _, k := range keys {
			args = append(args, k)
		}
	}
	res, err := u.Send(ctx, "MIGRATE", args...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// Object returns various information about a key.
func (u *Upstash) Object(ctx context.Context, subcommand, key string) (any, error) {
	return u.Send(ctx, "OBJECT", subcommand, key)
}

// Sort returns or stores the elements in a list, set or sorted set.
func (u *Upstash) Sort(ctx context.Context, key string, args ...any) (any, error) {
	fullArgs := make([]any, 0, 1+len(args))
	fullArgs = append(fullArgs, key)
	fullArgs = append(fullArgs, args...)
	return u.Send(ctx, "SORT", fullArgs...)
}

// SortRO is the read-only variant of SORT.
func (u *Upstash) SortRO(ctx context.Context, key string, args ...any) (any, error) {
	fullArgs := make([]any, 0, 1+len(args))
	fullArgs = append(fullArgs, key)
	fullArgs = append(fullArgs, args...)
	return u.Send(ctx, "SORT_RO", fullArgs...)
}

// ExpireTime returns the absolute Unix timestamp (in seconds) at which the given key will expire.
func (u *Upstash) ExpireTime(ctx context.Context, key string) (int64, error) {
	res, err := u.Send(ctx, "EXPIRETIME", key)
	if err != nil {
		return 0, err
	}
	return int64(res.(float64)), nil
}

// PExpireTime returns the absolute Unix timestamp (in milliseconds) at which the given key will expire.
func (u *Upstash) PExpireTime(ctx context.Context, key string) (int64, error) {
	res, err := u.Send(ctx, "PEXPIRETIME", key)
	if err != nil {
		return 0, err
	}
	return int64(res.(float64)), nil
}

// Wait blocks the current client until all the previous write commands are successfully transferred and acknowledged by at least the specified number of replicas.
func (u *Upstash) Wait(ctx context.Context, numReplicas int, timeout int64) (int, error) {
	res, err := u.Send(ctx, "WAIT", numReplicas, timeout)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Move moves a key from the currently selected database to the specified destination database.
func (u *Upstash) Move(ctx context.Context, key string, db int) (int, error) {
	res, err := u.Send(ctx, "MOVE", key, db)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Restore creates a key associated with a value that is obtained by deserializing the provided serialized value.
func (u *Upstash) Restore(ctx context.Context, key string, ttl int64, serializedValue string, replace bool) (string, error) {
	args := []any{key, ttl, serializedValue}
	if replace {
		args = append(args, "REPLACE")
	}
	res, err := u.Send(ctx, "RESTORE", args...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}
