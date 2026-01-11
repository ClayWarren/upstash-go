package upstash

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/claywarren/upstash-go/client"
)

// Upstash is a client for the Upstash Redis REST API.
type Upstash struct {
	client client.Client
}

// Options provides configuration for the Upstash client.
type Options struct {
	// Url is the Upstash endpoint you want to use.
	// Falls back to `UPSTASH_REDIS_REST_URL` environment variable.
	Url string

	// EdgeUrl is the Upstash edge url you want to use.
	// Falls back to `UPSTASH_REDIS_EDGE_URL` environment variable.
	EdgeUrl string

	// Token is the API token required for requests to the Upstash API.
	Token string

	// ReadFromEdge specifies if read requests should try to read from edge first.
	ReadFromEdge bool
}

// New creates a new Upstash client with the provided options.
func New(options Options) (Upstash, error) {
	if options.EdgeUrl == "" {
		options.EdgeUrl = os.Getenv("UPSTASH_REDIS_EDGE_URL")
	}

	if options.Url == "" {
		options.Url = os.Getenv("UPSTASH_REDIS_REST_URL")
	}
	if options.Token == "" {
		options.Token = os.Getenv("UPSTASH_REDIS_REST_TOKEN")
	}

	return Upstash{
		client: client.New(options.Url, options.EdgeUrl, options.Token),
	}, nil
}

// Keys returns all keys matching the provided pattern.
func (u *Upstash) Keys(ctx context.Context, pattern string) ([]string, error) {
	res, err := u.client.Read(ctx, client.Request{
		Body: []string{"keys", pattern},
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return []string{}, nil
	}

	// Handle conversion from []interface{} (which JSON decoder produces) to []string
	if list, ok := res.([]interface{}); ok {
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

// Append appends a value to a key. If the key does not exist, it is created as an empty string.
func (u *Upstash) Append(ctx context.Context, key string, value string) (int, error) {
	res, err := u.client.Write(ctx, client.Request{
		Body: []string{"append", key, value},
	})
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Decr decrements the number stored at key by one.
func (u *Upstash) Decr(ctx context.Context, key string) (int, error) {
	res, err := u.client.Write(ctx, client.Request{
		Body: []string{"decr", key},
	})
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// DecrBy decrements the number stored at key by the provided decrement value.
func (u *Upstash) DecrBy(ctx context.Context, key string, decrement int) (int, error) {
	res, err := u.client.Write(ctx, client.Request{
		Body: []string{"decrby", key, fmt.Sprintf("%d", decrement)},
	})
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Returns the value of key, or empty string when key does not exist.
//
// https://redis.io/commands/get
func (u *Upstash) Get(ctx context.Context, key string) (string, error) {

	res, err := u.client.Read(ctx, client.Request{
		Path: []string{"get", key},
	})
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}

	return res.(string), nil
}

// GetEx retrieves the value of a key and optionally sets its expiration.
// https://redis.io/commands/getex
func (u *Upstash) GetEx(ctx context.Context, key string, options GetEXOptions) (string, error) {
	body := []string{"getex", key}
	if options.EX != 0 {
		body = append(body, "ex", fmt.Sprintf("%d", options.EX))
	} else if options.PX != 0 {
		body = append(body, "px", fmt.Sprintf("%d", options.PX))
	} else if options.EXAT != 0 {
		body = append(body, "exat", fmt.Sprintf("%d", options.EXAT))
	} else if options.PXAT != 0 {
		body = append(body, "pxat", fmt.Sprintf("%d", options.PXAT))
	} else if options.PERSIST {
		body = append(body, "persist")
	}

	res, err := u.client.Write(ctx, client.Request{
		Body: body,
	})
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}

	return res.(string), nil
}

// GetRange returns a substring of the string value stored at a key.
func (u *Upstash) GetRange(ctx context.Context, key string, start int, end int) (string, error) {
	res, err := u.client.Read(ctx, client.Request{
		Path: []string{"getrange", key, fmt.Sprintf("%d", start), fmt.Sprintf("%d", end)},
	})
	if err != nil {
		return "", err
	}

	return res.(string), nil
}

// GetSet atomically sets a key to a value and returns the old value.
func (u *Upstash) GetSet(ctx context.Context, key string, value string) (string, error) {
	res, err := u.client.Write(ctx, client.Request{
		Body: []string{"getset", key, value},
	})
	if err != nil {
		return "", err
	}

	return res.(string), nil
}

// Incr increments the number stored at key by one.
func (u *Upstash) Incr(ctx context.Context, key string) (int, error) {
	res, err := u.client.Write(ctx, client.Request{
		Body: []string{"incr", key},
	})
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// IncrBy increments the number stored at key by the provided increment value.
func (u *Upstash) IncrBy(ctx context.Context, key string, increment int) (int, error) {
	res, err := u.client.Write(ctx, client.Request{
		Body: []string{"incrby", key, fmt.Sprintf("%d", increment)},
	})
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// IncrByFloat increments the string representing a floating point number stored at key by the provided increment.
func (u *Upstash) IncrByFloat(ctx context.Context, key string, increment float64) (float64, error) {
	res, err := u.client.Write(ctx, client.Request{
		Body: []string{"incrbyfloat", key, fmt.Sprintf("%f", increment)},
	})
	if err != nil {
		return 0, err
	}
	f, err := strconv.ParseFloat(res.(string), 64)
	if err != nil {
		return 0, err
	}
	return f, nil
}

// MGet returns the values of all specified keys.
func (u *Upstash) MGet(ctx context.Context, keys []string) ([]string, error) {
	res, err := u.client.Read(ctx, client.Request{
		Path: append([]string{"mget"}, keys...),
	})
	if err != nil {
		return nil, err
	}

	values := make([]string, len(keys))
	for i, value := range res.([]any) {
		values[i] = fmt.Sprint(value)
	}

	return values, err
}

// MSet sets the given keys to their respective values.
func (u *Upstash) MSet(ctx context.Context, kvPairs []KV) error {
	body := []string{"mset"}
	for _, kv := range kvPairs {
		body = append(body, kv.Key, kv.Value)
	}

	_, err := u.client.Write(ctx, client.Request{
		Body: body,
	})
	return err
}

// MSetNX sets the given keys to their respective values if none of the keys exist.
func (u *Upstash) MSetNX(ctx context.Context, kvPairs []KV) (int, error) {
	body := []string{"msetnx"}
	for _, kv := range kvPairs {
		body = append(body, kv.Key, kv.Value)
	}

	res, err := u.client.Write(ctx, client.Request{
		Body: body,
	})
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return int(res.(float64)), nil
}

// PSetEX sets a key to a value with a provided expiration time in milliseconds.
func (u *Upstash) PSetEX(ctx context.Context, key string, milliseconds int, value string) error {
	_, err := u.client.Write(ctx, client.Request{
		Body: []string{"psetex", key, fmt.Sprintf("%d", milliseconds), value},
	})
	return err
}

// Set sets a key to hold the string value.
func (u *Upstash) Set(ctx context.Context, key string, value string) error {
	_, err := u.client.Write(ctx, client.Request{
		Body: []string{"set", key, value},
	})
	return err
}

// SetWithOptions sets a key to hold the string value with additional options.
func (u *Upstash) SetWithOptions(ctx context.Context, key string, value string, options SetOptions) error {
	body := []string{"set", key, value}
	if options.EX != 0 {
		body = append(body, "ex", fmt.Sprintf("%d", options.EX))
	} else if options.PX != 0 {
		body = append(body, "px", fmt.Sprintf("%d", options.PX))
	}
	if options.NX {
		body = append(body, "nx")
	} else if options.XX {
		body = append(body, "xx")
	}

	_, err := u.client.Write(ctx, client.Request{
		Body: body,
	})
	if err != nil {
		return fmt.Errorf("error %s: %w", body, err)
	}
	return nil
}

// SetEX sets a key to hold the string value with a provided expiration time in seconds.
func (u *Upstash) SetEX(ctx context.Context, key string, seconds int, value string) error {
	_, err := u.client.Write(ctx, client.Request{
		Body: []string{"setex", key, fmt.Sprintf("%d", seconds), value},
	})
	return err
}

// SetNX sets a key to hold the string value if the key does not exist.
func (u *Upstash) SetNX(ctx context.Context, key string, value string) (int, error) {
	res, err := u.client.Write(ctx, client.Request{
		Body: []string{"setnx", key, value},
	})
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// SetRange overwrites part of the string stored at a key, starting at the specified offset.
func (u *Upstash) SetRange(ctx context.Context, key string, offset int, value string) error {
	_, err := u.client.Write(ctx, client.Request{
		Body: []string{"setrange", key, fmt.Sprintf("%d", offset), value},
	})
	return err
}

// StrLen returns the length of the string value stored at a key.
func (u *Upstash) StrLen(ctx context.Context, key string) (int, error) {
	res, err := u.client.Read(ctx, client.Request{
		Path: []string{"strlen", key},
	})

	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// FlushAll deletes all keys of all existing databases.
func (u *Upstash) FlushAll(ctx context.Context) error {
	_, err := u.client.Write(ctx, client.Request{
		Body: []string{"flushall"},
	})
	return err
}
