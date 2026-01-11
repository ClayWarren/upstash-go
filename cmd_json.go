package upstash

import (
	"context"
)

// JsonSet sets the JSON value at path in key.
func (u *Upstash) JsonSet(ctx context.Context, key, path string, value any) (string, error) {
	res, err := u.Send(ctx, "JSON.SET", key, path, value)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// JsonGet returns the value at path in key.
func (u *Upstash) JsonGet(ctx context.Context, key string, paths ...string) (any, error) {
	args := make([]any, 0, 1+len(paths))
	args = append(args, key)
	for _, p := range paths {
		args = append(args, p)
	}
	return u.Send(ctx, "JSON.GET", args...)
}

// JsonDel deletes the value at path in key.
func (u *Upstash) JsonDel(ctx context.Context, key, path string) (int, error) {
	res, err := u.Send(ctx, "JSON.DEL", key, path)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}
