package upstash

import (
	"context"
)

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
