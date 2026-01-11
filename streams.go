package upstash

import (
	"context"
)

// XAdd appends the specified stream entry to the stream at key.
func (u *Upstash) XAdd(ctx context.Context, key, id string, values map[string]string) (string, error) {
	args := make([]any, 0, 2+len(values)*2)
	args = append(args, key, id)
	for k, v := range values {
		args = append(args, k, v)
	}
	res, err := u.Send(ctx, "XADD", args...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// XLen returns the number of entries of a stream.
func (u *Upstash) XLen(ctx context.Context, key string) (int, error) {
	res, err := u.Send(ctx, "XLEN", key)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}
