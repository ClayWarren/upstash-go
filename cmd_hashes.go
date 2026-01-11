package upstash

import (
	"context"
	"strconv"
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

// HExists returns if field is an existing field in the hash stored at key.
func (u *Upstash) HExists(ctx context.Context, key, field string) (int, error) {
	res, err := u.Send(ctx, "HEXISTS", key, field)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// HIncrBy increments the integer value of a hash field by the given number.
func (u *Upstash) HIncrBy(ctx context.Context, key, field string, increment int) (int, error) {
	res, err := u.Send(ctx, "HINCRBY", key, field, increment)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// HIncrByFloat increments the float value of a hash field by the given amount.
func (u *Upstash) HIncrByFloat(ctx context.Context, key, field string, increment float64) (float64, error) {
	res, err := u.Send(ctx, "HINCRBYFLOAT", key, field, increment)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(res.(string), 64)
}

// HKeys returns all field names in the hash stored at key.
func (u *Upstash) HKeys(ctx context.Context, key string) ([]string, error) {
	res, err := u.Send(ctx, "HKEYS", key)
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

// HMGet returns the values associated with the specified fields in the hash stored at key.
func (u *Upstash) HMGet(ctx context.Context, key string, fields ...string) ([]string, error) {
	args := make([]any, 0, 1+len(fields))
	args = append(args, key)
	for _, f := range fields {
		args = append(args, f)
	}
	res, err := u.Send(ctx, "HMGET", args...)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([]string, len(list))
	for i, v := range list {
		if v == nil {
			result[i] = ""
		} else {
			result[i] = v.(string)
		}
	}
	return result, nil
}

// HMSet sets the specified fields to their respective values in the hash stored at key.
func (u *Upstash) HMSet(ctx context.Context, key string, kv map[string]string) (string, error) {
	args := make([]any, 0, 1+len(kv)*2)
	args = append(args, key)
	for k, v := range kv {
		args = append(args, k, v)
	}
	res, err := u.Send(ctx, "HMSET", args...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// HSetNX sets the value of a hash field, only if the field does not yet exist.
func (u *Upstash) HSetNX(ctx context.Context, key, field, value string) (int, error) {
	res, err := u.Send(ctx, "HSETNX", key, field, value)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// HStrLen returns the string length of the value associated with field in the hash stored at key.
func (u *Upstash) HStrLen(ctx context.Context, key, field string) (int, error) {
	res, err := u.Send(ctx, "HSTRLEN", key, field)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// HVals returns all values in the hash stored at key.
func (u *Upstash) HVals(ctx context.Context, key string) ([]string, error) {
	res, err := u.Send(ctx, "HVALS", key)
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
