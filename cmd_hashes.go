package upstash

import (
	"context"
)

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

// HScan iterates over fields of a hash.
func (u *Upstash) HScan(ctx context.Context, key, cursor string, options ScanOptions) (ScanResult, error) {
	return u.scan(ctx, key, cursor, options, "HSCAN")
}
