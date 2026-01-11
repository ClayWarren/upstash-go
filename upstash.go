package upstash

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

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

	// EnableBase64 specifies if strings in the response should be base64 encoded.
	// The client will automatically decode these back to raw strings.
	EnableBase64 bool
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
		client: client.New(options.Url, options.EdgeUrl, options.Token, options.EnableBase64),
	}, nil
}

// Keys returns all keys matching the provided pattern.
func (u *Upstash) Keys(ctx context.Context, pattern string) ([]string, error) {
	res, err := u.client.Read(ctx, client.Request{
		Path: []string{"keys", pattern},
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

// Get retrieves the value of a key.
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

// Send executes an arbitrary Redis command.
// It returns the raw response from the Upstash REST API.
// Use this for commands that are not yet explicitly typed in this library (e.g. HSET, LPOP).
func (u *Upstash) Send(ctx context.Context, command string, args ...any) (any, error) {
	// Construct the command body: [COMMAND, arg1, arg2, ...]
	body := make([]any, 0, 1+len(args))
	body = append(body, command)
	body = append(body, args...)

	res, err := u.client.Write(ctx, client.Request{
		Body: body,
	})
	return res, err
}

// Pipeline represents a sequence of commands to be executed via Upstash pipeline.
type Pipeline struct {
	commands [][]any
	client   client.Client
}

// Pipeline creates a new Pipeline.
func (u *Upstash) Pipeline() *Pipeline {
	return &Pipeline{
		commands: make([][]any, 0),
		client:   u.client,
	}
}

// Push adds a command to the pipeline.
func (p *Pipeline) Push(command string, args ...any) {
	cmd := make([]any, 0, 1+len(args))
	cmd = append(cmd, command)
	cmd = append(cmd, args...)
	p.commands = append(p.commands, cmd)
}

// Exec executes the queued commands in the pipeline.
// Returns an array of results corresponding to the commands.
func (p *Pipeline) Exec(ctx context.Context) ([]any, error) {
	// Send to /pipeline
	res, err := p.client.Write(ctx, client.Request{
		Path: []string{"pipeline"},
		Body: p.commands,
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	// Pipeline returns an array of results
	if list, ok := res.([]any); ok {
		return list, nil
	}
	return nil, fmt.Errorf("unexpected return type for pipeline: %T", res)
}

// Multi represents a sequence of commands to be executed as a transaction.
type Multi struct {
	commands [][]any
	client   client.Client
}

// Multi creates a new Multi (Transaction).
func (u *Upstash) Multi() *Multi {
	return &Multi{
		commands: make([][]any, 0),
		client:   u.client,
	}
}

// Push adds a command to the transaction.
func (m *Multi) Push(command string, args ...any) {
	cmd := make([]any, 0, 1+len(args))
	cmd = append(cmd, command)
	cmd = append(cmd, args...)
	m.commands = append(m.commands, cmd)
}

// Exec executes the queued commands in the transaction.
// Returns an array of results corresponding to the commands.
func (m *Multi) Exec(ctx context.Context) ([]any, error) {
	// Send to /multi-exec
	res, err := m.client.Write(ctx, client.Request{
		Path: []string{"multi-exec"},
		Body: m.commands,
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	// Transaction returns an array of results
	if list, ok := res.([]any); ok {
		return list, nil
	}
	return nil, fmt.Errorf("unexpected return type for multi-exec: %T", res)
}

// HSet sets the string value of a hash field.
func (u *Upstash) HSet(ctx context.Context, key, field, value string) (int, error) {
	res, err := u.Send(ctx, "HSET", key, field, value)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// HGet returns the value associated with field in the hash stored at key.
func (u *Upstash) HGet(ctx context.Context, key, field string) (string, error) {
	res, err := u.Send(ctx, "HGET", key, field)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return res.(string), nil
}

// HGetAll returns all fields and values of the hash stored at key.
func (u *Upstash) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	res, err := u.Send(ctx, "HGETALL", key)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make(map[string]string, len(list)/2)
	for i := 0; i < len(list); i += 2 {
		result[list[i].(string)] = list[i+1].(string)
	}
	return result, nil
}

// HDel deletes one or more hash fields.
func (u *Upstash) HDel(ctx context.Context, key string, fields ...string) (int, error) {
	args := make([]any, 0, 1+len(fields))
	args = append(args, key)
	for _, f := range fields {
		args = append(args, f)
	}
	res, err := u.Send(ctx, "HDEL", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// HLen returns the number of fields contained in the hash stored at key.
func (u *Upstash) HLen(ctx context.Context, key string) (int, error) {
	res, err := u.Send(ctx, "HLEN", key)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// LPush inserts all the specified values at the head of the list stored at key.
func (u *Upstash) LPush(ctx context.Context, key string, values ...string) (int, error) {
	args := make([]any, 0, 1+len(values))
	args = append(args, key)
	for _, v := range values {
		args = append(args, v)
	}
	res, err := u.Send(ctx, "LPUSH", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// RPush inserts all the specified values at the tail of the list stored at key.
func (u *Upstash) RPush(ctx context.Context, key string, values ...string) (int, error) {
	args := make([]any, 0, 1+len(values))
	args = append(args, key)
	for _, v := range values {
		args = append(args, v)
	}
	res, err := u.Send(ctx, "RPUSH", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// LPop removes and returns the first element of the list stored at key.
func (u *Upstash) LPop(ctx context.Context, key string) (string, error) {
	res, err := u.Send(ctx, "LPOP", key)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return res.(string), nil
}

// RPop removes and returns the last element of the list stored at key.
func (u *Upstash) RPop(ctx context.Context, key string) (string, error) {
	res, err := u.Send(ctx, "RPOP", key)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return res.(string), nil
}

// LLen returns the length of the list stored at key.
func (u *Upstash) LLen(ctx context.Context, key string) (int, error) {
	res, err := u.Send(ctx, "LLEN", key)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Scan iterates over the keys in the database.
func (u *Upstash) Scan(ctx context.Context, cursor string, options ScanOptions) (ScanResult, error) {
	return u.scan(ctx, "", cursor, options, "SCAN")
}

// HScan iterates over fields of a hash.
func (u *Upstash) HScan(ctx context.Context, key, cursor string, options ScanOptions) (ScanResult, error) {
	return u.scan(ctx, key, cursor, options, "HSCAN")
}

// SScan iterates over members of a set.
func (u *Upstash) SScan(ctx context.Context, key, cursor string, options ScanOptions) (ScanResult, error) {
	return u.scan(ctx, key, cursor, options, "SSCAN")
}

// ZScan iterates over members of a sorted set.
func (u *Upstash) ZScan(ctx context.Context, key, cursor string, options ScanOptions) (ScanResult, error) {
	return u.scan(ctx, key, cursor, options, "ZSCAN")
}

func (u *Upstash) scan(ctx context.Context, key, cursor string, options ScanOptions, command string) (ScanResult, error) {
	args := make([]any, 0)
	if key != "" {
		args = append(args, key)
	}
	args = append(args, cursor)
	if options.Match != "" {
		args = append(args, "MATCH", options.Match)
	}
	if options.Count != 0 {
		args = append(args, "COUNT", options.Count)
	}
	if options.Type != "" && command == "SCAN" {
		args = append(args, "TYPE", options.Type)
	}

	res, err := u.Send(ctx, command, args...)
	if err != nil {
		return ScanResult{}, err
	}

	list := res.([]any)
	cursorOut := fmt.Sprint(list[0])
	itemsRaw := list[1].([]any)
	items := make([]string, len(itemsRaw))
	for i, v := range itemsRaw {
		items[i] = fmt.Sprint(v)
	}

	return ScanResult{
		Cursor: cursorOut,
		Items:  items,
	}, nil
}

// PFAdd adds elements to a HyperLogLog.
func (u *Upstash) PFAdd(ctx context.Context, key string, elements ...string) (int, error) {
	args := make([]any, 0, 1+len(elements))
	args = append(args, key)
	for _, e := range elements {
		args = append(args, e)
	}
	res, err := u.Send(ctx, "PFADD", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// PFCount returns the approximated cardinality of the HyperLogLog(s).
func (u *Upstash) PFCount(ctx context.Context, keys ...string) (int, error) {
	args := make([]any, 0, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "PFCOUNT", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// PFMerge merges multiple HyperLogLogs into one.
func (u *Upstash) PFMerge(ctx context.Context, dest string, sources ...string) error {
	args := make([]any, 0, 1+len(sources))
	args = append(args, dest)
	for _, s := range sources {
		args = append(args, s)
	}
	_, err := u.Send(ctx, "PFMERGE", args...)
	return err
}

// SetBit sets or clears the bit at offset in the string value stored at key.
func (u *Upstash) SetBit(ctx context.Context, key string, offset int, value int) (int, error) {
	res, err := u.Send(ctx, "SETBIT", key, offset, value)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// GetBit returns the bit value at offset in the string value stored at key.
func (u *Upstash) GetBit(ctx context.Context, key string, offset int) (int, error) {
	res, err := u.Send(ctx, "GETBIT", key, offset)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// BitCount counts the number of set bits (population counting) in a string.
func (u *Upstash) BitCount(ctx context.Context, key string) (int, error) {
	res, err := u.Send(ctx, "BITCOUNT", key)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// SAdd adds one or more members to a set.
func (u *Upstash) SAdd(ctx context.Context, key string, members ...string) (int, error) {
	args := make([]any, 0, 1+len(members))
	args = append(args, key)
	for _, m := range members {
		args = append(args, m)
	}
	res, err := u.Send(ctx, "SADD", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// SRem removes one or more members from a set.
func (u *Upstash) SRem(ctx context.Context, key string, members ...string) (int, error) {
	args := make([]any, 0, 1+len(members))
	args = append(args, key)
	for _, m := range members {
		args = append(args, m)
	}
	res, err := u.Send(ctx, "SREM", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// SIsMember returns if member is a member of the set stored at key.
func (u *Upstash) SIsMember(ctx context.Context, key, member string) (int, error) {
	res, err := u.Send(ctx, "SISMEMBER", key, member)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// SMembers returns all the members of the set value stored at key.
func (u *Upstash) SMembers(ctx context.Context, key string) ([]string, error) {
	res, err := u.Send(ctx, "SMEMBERS", key)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([]string, len(list))
	for i, v := range list {
		result[i] = v.(string)
	}
	return result, nil
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (u *Upstash) SCard(ctx context.Context, key string) (int, error) {
	res, err := u.Send(ctx, "SCARD", key)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// ZAdd adds all the specified members with the specified scores to the sorted set stored at key.
func (u *Upstash) ZAdd(ctx context.Context, key string, score float64, member string) (int, error) {
	res, err := u.Send(ctx, "ZADD", key, score, member)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// ZRem removes the specified members from the sorted set stored at key.
func (u *Upstash) ZRem(ctx context.Context, key string, members ...string) (int, error) {
	args := make([]any, 0, 1+len(members))
	args = append(args, key)
	for _, m := range members {
		args = append(args, m)
	}
	res, err := u.Send(ctx, "ZREM", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// ZRange returns the specified range of elements in the sorted set stored at key.
func (u *Upstash) ZRange(ctx context.Context, key string, start, stop int) ([]string, error) {
	res, err := u.Send(ctx, "ZRANGE", key, start, stop)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([]string, len(list))
	for i, v := range list {
		result[i] = v.(string)
	}
	return result, nil
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (u *Upstash) ZCard(ctx context.Context, key string) (int, error) {
	res, err := u.Send(ctx, "ZCARD", key)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// ZScore returns the score of member in the sorted set at key.
func (u *Upstash) ZScore(ctx context.Context, key, member string) (float64, error) {
	res, err := u.Send(ctx, "ZSCORE", key, member)
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return strconv.ParseFloat(res.(string), 64)
}

// Publish posts a message to the given channel.
func (u *Upstash) Publish(ctx context.Context, channel, message string) (int, error) {
	res, err := u.Send(ctx, "PUBLISH", channel, message)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Subscribe subscribes to a channel and returns a channel of messages.
func (u *Upstash) Subscribe(ctx context.Context, channel string) (<-chan string, error) {
	stream, err := u.client.Stream(ctx, client.Request{
		Path: []string{"subscribe", channel},
	})
	if err != nil {
		return nil, err
	}

	out := make(chan string)
	go u.streamReader(ctx, stream, out)
	return out, nil
}

// Monitor monitors all commands hitting the database in real-time.
func (u *Upstash) Monitor(ctx context.Context) (<-chan string, error) {
	stream, err := u.client.Stream(ctx, client.Request{
		Path: []string{"monitor"},
	})
	if err != nil {
		return nil, err
	}

	out := make(chan string)
	go u.streamReader(ctx, stream, out)
	return out, nil
}

func (u *Upstash) streamReader(ctx context.Context, stream io.ReadCloser, out chan<- string) {
	defer func() {
		_ = stream.Close()
	}()
	defer close(out)

	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			msg := strings.TrimPrefix(line, "data: ")
			// Upstash might wrap the data in quotes if it's a string from JSON
			if strings.HasPrefix(msg, "\"") && strings.HasSuffix(msg, "\"") && len(msg) >= 2 {
				msg = msg[1 : len(msg)-1]
			}
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
