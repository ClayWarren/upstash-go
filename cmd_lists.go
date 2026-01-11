package upstash

import (
	"context"
)

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

// LIndex returns the element at index index in the list stored at key.
func (u *Upstash) LIndex(ctx context.Context, key string, index int) (string, error) {
	res, err := u.Send(ctx, "LINDEX", key, index)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return res.(string), nil
}

// LInsert inserts element in the list stored at key either before or after the reference value pivot.
func (u *Upstash) LInsert(ctx context.Context, key, op, pivot, element string) (int, error) {
	res, err := u.Send(ctx, "LINSERT", key, op, pivot, element)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// LMove atomically returns and removes the first/last element of the list stored at source,
// and pushes the element at the first/last element of the list stored at destination.
func (u *Upstash) LMove(ctx context.Context, source, destination, srcPos, destPos string) (string, error) {
	res, err := u.Send(ctx, "LMOVE", source, destination, srcPos, destPos)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return res.(string), nil
}

// LPos returns the index of matching elements inside a list.
func (u *Upstash) LPos(ctx context.Context, key, element string) (int, error) {
	res, err := u.Send(ctx, "LPOS", key, element)
	if err != nil {
		return 0, err
	}
	if res == nil {
		return -1, nil
	}
	return int(res.(float64)), nil
}

// LPushX inserts value at the head of the list stored at key, only if key already exists and holds a list.
func (u *Upstash) LPushX(ctx context.Context, key string, values ...string) (int, error) {
	args := make([]any, 0, 1+len(values))
	args = append(args, key)
	for _, v := range values {
		args = append(args, v)
	}
	res, err := u.Send(ctx, "LPUSHX", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// LRange returns the specified elements of the list stored at key.
func (u *Upstash) LRange(ctx context.Context, key string, start, stop int) ([]string, error) {
	res, err := u.Send(ctx, "LRANGE", key, start, stop)
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

// LRem removes the first count occurrences of elements equal to value from the list stored at key.
func (u *Upstash) LRem(ctx context.Context, key string, count int, value string) (int, error) {
	res, err := u.Send(ctx, "LREM", key, count, value)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// LSet sets the list element at index to value.
func (u *Upstash) LSet(ctx context.Context, key string, index int, value string) (string, error) {
	res, err := u.Send(ctx, "LSET", key, index, value)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// LTrim trims an existing list so that it will contain only the specified range of elements specified.
func (u *Upstash) LTrim(ctx context.Context, key string, start, stop int) (string, error) {
	res, err := u.Send(ctx, "LTRIM", key, start, stop)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// RPopLPush atomically returns and removes the last element of the list stored at source,
// and pushes the element at the first element of the list stored at destination.
func (u *Upstash) RPopLPush(ctx context.Context, source, destination string) (string, error) {
	res, err := u.Send(ctx, "RPOPLPUSH", source, destination)
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return res.(string), nil
}

// BLPop is a blocking list pop primitive.
func (u *Upstash) BLPop(ctx context.Context, timeout int64, keys ...string) ([]string, error) {
	args := make([]any, 0, len(keys)+1)
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, timeout)
	res, err := u.Send(ctx, "BLPOP", args...)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	list := res.([]any)
	result := make([]string, len(list))
	for i, v := range list {
		result[i] = v.(string)
	}
	return result, nil
}

// BRPop is a blocking list pop primitive.
func (u *Upstash) BRPop(ctx context.Context, timeout int64, keys ...string) ([]string, error) {
	args := make([]any, 0, len(keys)+1)
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, timeout)
	res, err := u.Send(ctx, "BRPOP", args...)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	list := res.([]any)
	result := make([]string, len(list))
	for i, v := range list {
		result[i] = v.(string)
	}
	return result, nil
}

// RPushX inserts value at the tail of the list stored at key, only if key already exists and holds a list.
func (u *Upstash) RPushX(ctx context.Context, key string, values ...string) (int, error) {
	args := make([]any, 0, 1+len(values))
	args = append(args, key)
	for _, v := range values {
		args = append(args, v)
	}
	res, err := u.Send(ctx, "RPUSHX", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}
