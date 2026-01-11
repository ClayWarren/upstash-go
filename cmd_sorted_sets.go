package upstash

import (
	"context"
	"strconv"
)

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

// ZScan iterates over members of a sorted set.
func (u *Upstash) ZScan(ctx context.Context, key, cursor string, options ScanOptions) (ScanResult, error) {
	return u.scan(ctx, key, cursor, options, "ZSCAN")
}
