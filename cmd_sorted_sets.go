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

// ZCount returns the number of elements in the sorted set at key with a score between min and max.
func (u *Upstash) ZCount(ctx context.Context, key string, min, max any) (int, error) {
	res, err := u.Send(ctx, "ZCOUNT", key, min, max)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// ZDiff returns the difference between the first sorted set and all successive sorted sets.
func (u *Upstash) ZDiff(ctx context.Context, keys ...string) ([]string, error) {
	args := make([]any, 0, 1+len(keys))
	args = append(args, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "ZDIFF", args...)
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

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
func (u *Upstash) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	res, err := u.Send(ctx, "ZINCRBY", key, increment, member)
	if err != nil {
		return 0, err
	}
	return strconv.ParseFloat(res.(string), 64)
}

// ZLexCount returns the number of elements in the sorted set at key with a value between min and max.
func (u *Upstash) ZLexCount(ctx context.Context, key, min, max string) (int, error) {
	res, err := u.Send(ctx, "ZLEXCOUNT", key, min, max)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// ZMScore returns the scores associated with the specified members in the sorted set stored at key.
func (u *Upstash) ZMScore(ctx context.Context, key string, members ...string) ([]float64, error) {
	args := make([]any, 0, 1+len(members))
	args = append(args, key)
	for _, m := range members {
		args = append(args, m)
	}
	res, err := u.Send(ctx, "ZMSCORE", args...)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([]float64, len(list))
	for i, v := range list {
		if v == nil {
			result[i] = 0
		} else {
			result[i], _ = strconv.ParseFloat(v.(string), 64)
		}
	}
	return result, nil
}

// ZPopMax removes and returns the member with the highest score from the sorted set stored at key.
func (u *Upstash) ZPopMax(ctx context.Context, key string, count ...int) ([]string, error) {
	args := make([]any, 0, 1+len(count))
	args = append(args, key)
	if len(count) > 0 {
		args = append(args, count[0])
	}
	res, err := u.Send(ctx, "ZPOPMAX", args...)
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

// ZPopMin removes and returns the member with the lowest score from the sorted set stored at key.
func (u *Upstash) ZPopMin(ctx context.Context, key string, count ...int) ([]string, error) {
	args := make([]any, 0, 1+len(count))
	args = append(args, key)
	if len(count) > 0 {
		args = append(args, count[0])
	}
	res, err := u.Send(ctx, "ZPOPMIN", args...)
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

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
func (u *Upstash) ZRank(ctx context.Context, key, member string) (int, error) {
	res, err := u.Send(ctx, "ZRANK", key, member)
	if err != nil {
		return 0, err
	}
	if res == nil {
		return -1, nil
	}
	return int(res.(float64)), nil
}

// ZRemRangeByLex removes all elements in the sorted set stored at key between the lexicographical range specified by min and max.
func (u *Upstash) ZRemRangeByLex(ctx context.Context, key, min, max string) (int, error) {
	res, err := u.Send(ctx, "ZREMRANGEBYLEX", key, min, max)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// ZRemRangeByRank removes all elements in the sorted set stored at key with rank between start and stop.
func (u *Upstash) ZRemRangeByRank(ctx context.Context, key string, start, stop int) (int, error) {
	res, err := u.Send(ctx, "ZREMRANGEBYRANK", key, start, stop)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// ZRemRangeByScore removes all elements in the sorted set stored at key with a score between min and max.
func (u *Upstash) ZRemRangeByScore(ctx context.Context, key string, min, max any) (int, error) {
	res, err := u.Send(ctx, "ZREMRANGEBYSCORE", key, min, max)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// ZRevRange returns the specified range of elements in the sorted set stored at key, with the scores ordered from high to low.
func (u *Upstash) ZRevRange(ctx context.Context, key string, start, stop int) ([]string, error) {
	res, err := u.Send(ctx, "ZREVRANGE", key, start, stop)
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

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
func (u *Upstash) ZRevRank(ctx context.Context, key, member string) (int, error) {
	res, err := u.Send(ctx, "ZREVRANK", key, member)
	if err != nil {
		return 0, err
	}
	if res == nil {
		return -1, nil
	}
	return int(res.(float64)), nil
}

// BZPopMax is a blocking variant of ZPOPMAX.
func (u *Upstash) BZPopMax(ctx context.Context, timeout int64, keys ...string) ([]string, error) {
	args := make([]any, 0, len(keys)+1)
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, timeout)
	res, err := u.Send(ctx, "BZPOPMAX", args...)
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

// BZPopMin is a blocking variant of ZPOPMIN.
func (u *Upstash) BZPopMin(ctx context.Context, timeout int64, keys ...string) ([]string, error) {
	args := make([]any, 0, len(keys)+1)
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, timeout)
	res, err := u.Send(ctx, "BZPOPMIN", args...)
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

// ZUnion returns the union of multiple sorted sets.
func (u *Upstash) ZUnion(ctx context.Context, keys ...string) ([]string, error) {
	args := make([]any, 0, 1+len(keys))
	args = append(args, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "ZUNION", args...)
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

// ZUnionStore is equal to ZUNION, but instead of returning the resulting set, it is stored in destination.
func (u *Upstash) ZUnionStore(ctx context.Context, destination string, keys ...string) (int, error) {
	args := make([]any, 0, 2+len(keys))
	args = append(args, destination, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "ZUNIONSTORE", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// ZRevRangeByLex returns all the elements in the sorted set at key with a value between max and min.
func (u *Upstash) ZRevRangeByLex(ctx context.Context, key, max, min string, count ...int) ([]string, error) {
	args := make([]any, 0, 3+len(count)*2)
	args = append(args, key, max, min)
	if len(count) > 0 {
		args = append(args, "LIMIT", 0, count[0])
	}
	res, err := u.Send(ctx, "ZREVRANGEBYLEX", args...)
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

// ZRevRangeByScore returns all the elements in the sorted set at key with a score between max and min.
func (u *Upstash) ZRevRangeByScore(ctx context.Context, key string, max, min any, count ...int) ([]string, error) {
	args := make([]any, 0, 3+len(count)*2)
	args = append(args, key, max, min)
	if len(count) > 0 {
		args = append(args, "LIMIT", 0, count[0])
	}
	res, err := u.Send(ctx, "ZREVRANGEBYSCORE", args...)
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
