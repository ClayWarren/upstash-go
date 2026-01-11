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
