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
