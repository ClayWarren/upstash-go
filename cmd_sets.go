package upstash

import (
	"context"
)

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

// SScan iterates over members of a set.
func (u *Upstash) SScan(ctx context.Context, key, cursor string, options ScanOptions) (ScanResult, error) {
	return u.scan(ctx, key, cursor, options, "SSCAN")
}

// SDiff returns the members of the set resulting from the difference between the first set and all the successive sets.
func (u *Upstash) SDiff(ctx context.Context, keys ...string) ([]string, error) {
	args := make([]any, 0, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "SDIFF", args...)
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

// SDiffStore is equal to SDIFF, but instead of returning the resulting set, it is stored in destination.
func (u *Upstash) SDiffStore(ctx context.Context, destination string, keys ...string) (int, error) {
	args := make([]any, 0, 1+len(keys))
	args = append(args, destination)
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "SDIFFSTORE", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// SInter returns the members of the set resulting from the intersection of all the given sets.
func (u *Upstash) SInter(ctx context.Context, keys ...string) ([]string, error) {
	args := make([]any, 0, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "SINTER", args...)
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

// SInterStore is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
func (u *Upstash) SInterStore(ctx context.Context, destination string, keys ...string) (int, error) {
	args := make([]any, 0, 1+len(keys))
	args = append(args, destination)
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "SINTERSTORE", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// SMove moves member from the set at source to the set at destination.
func (u *Upstash) SMove(ctx context.Context, source, destination, member string) (int, error) {
	res, err := u.Send(ctx, "SMOVE", source, destination, member)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// SPop removes and returns one or more random members from the set value store at key.
func (u *Upstash) SPop(ctx context.Context, key string, count ...int) (any, error) {
	args := make([]any, 0, 1+len(count))
	args = append(args, key)
	if len(count) > 0 {
		args = append(args, count[0])
	}
	return u.Send(ctx, "SPOP", args...)
}

// SRandMember returns one or more random members from the set value store at key.
func (u *Upstash) SRandMember(ctx context.Context, key string, count ...int) (any, error) {
	args := make([]any, 0, 1+len(count))
	args = append(args, key)
	if len(count) > 0 {
		args = append(args, count[0])
	}
	return u.Send(ctx, "SRANDMEMBER", args...)
}

// SUnion returns the members of the set resulting from the union of all the given sets.
func (u *Upstash) SUnion(ctx context.Context, keys ...string) ([]string, error) {
	args := make([]any, 0, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "SUNION", args...)
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

// SUnionStore is equal to SUNION, but instead of returning the resulting set, it is stored in destination.
func (u *Upstash) SUnionStore(ctx context.Context, destination string, keys ...string) (int, error) {
	args := make([]any, 0, 1+len(keys))
	args = append(args, destination)
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "SUNIONSTORE", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// SMIsMember returns whether the members are members of the set stored at key.
func (u *Upstash) SMIsMember(ctx context.Context, key string, members ...string) ([]int, error) {
	args := make([]any, 0, 1+len(members))
	args = append(args, key)
	for _, m := range members {
		args = append(args, m)
	}
	res, err := u.Send(ctx, "SMISMEMBER", args...)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([]int, len(list))
	for i, v := range list {
		result[i] = int(v.(float64))
	}
	return result, nil
}
