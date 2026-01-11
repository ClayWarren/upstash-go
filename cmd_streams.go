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

// XRange returns the stream entries matching a range of IDs.
func (u *Upstash) XRange(ctx context.Context, key, start, stop string, count ...int) ([]StreamMessage, error) {
	args := make([]any, 0, 3+len(count)*2)
	args = append(args, key, start, stop)
	if len(count) > 0 {
		args = append(args, "COUNT", count[0])
	}
	res, err := u.Send(ctx, "XRANGE", args...)
	if err != nil {
		return nil, err
	}
	return u.parseStreamMessages(res)
}

// XRevRange returns the stream entries matching a range of IDs in reverse order.
func (u *Upstash) XRevRange(ctx context.Context, key, stop, start string, count ...int) ([]StreamMessage, error) {
	args := make([]any, 0, 3+len(count)*2)
	args = append(args, key, stop, start)
	if len(count) > 0 {
		args = append(args, "COUNT", count[0])
	}
	res, err := u.Send(ctx, "XREVRANGE", args...)
	if err != nil {
		return nil, err
	}
	return u.parseStreamMessages(res)
}

func (u *Upstash) parseStreamMessages(res any) ([]StreamMessage, error) {
	if res == nil {
		return []StreamMessage{}, nil
	}
	list := res.([]any)
	result := make([]StreamMessage, len(list))
	for i, v := range list {
		entry := v.([]any)
		id := entry[0].(string)
		fieldsRaw := entry[1].([]any)
		fields := make(map[string]string, len(fieldsRaw)/2)
		for j := 0; j < len(fieldsRaw); j += 2 {
			fields[fieldsRaw[j].(string)] = fieldsRaw[j+1].(string)
		}
		result[i] = StreamMessage{
			ID:     id,
			Values: fields,
		}
	}
	return result, nil
}

// XAck acknowledges one or more messages as being correctly processed by the consumer group.
func (u *Upstash) XAck(ctx context.Context, key, group string, ids ...string) (int, error) {
	args := make([]any, 0, 2+len(ids))
	args = append(args, key, group)
	for _, id := range ids {
		args = append(args, id)
	}
	res, err := u.Send(ctx, "XACK", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// XDel removes the specified entries from a stream.
func (u *Upstash) XDel(ctx context.Context, key string, ids ...string) (int, error) {
	args := make([]any, 0, 1+len(ids))
	args = append(args, key)
	for _, id := range ids {
		args = append(args, id)
	}
	res, err := u.Send(ctx, "XDEL", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// XGroup manages consumer groups.
func (u *Upstash) XGroup(ctx context.Context, subcommand string, key string, groupname string, args ...any) (any, error) {
	fullArgs := make([]any, 0, 3+len(args))
	fullArgs = append(fullArgs, subcommand, key, groupname)
	fullArgs = append(fullArgs, args...)
	return u.Send(ctx, "XGROUP", fullArgs...)
}

// XRead reads data from one or multiple streams.
func (u *Upstash) XRead(ctx context.Context, count int, block int, streams map[string]string) (any, error) {
	args := make([]any, 0)
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	if block >= 0 {
		args = append(args, "BLOCK", block)
	}
	args = append(args, "STREAMS")
	keys := make([]any, 0, len(streams))
	ids := make([]any, 0, len(streams))
	for k, v := range streams {
		keys = append(keys, k)
		ids = append(ids, v)
	}
	args = append(args, keys...)
	args = append(args, ids...)
	return u.Send(ctx, "XREAD", args...)
}

// XTrim trims the stream to a different length.
func (u *Upstash) XTrim(ctx context.Context, key string, strategy string, threshold any, args ...any) (int, error) {
	fullArgs := make([]any, 0, 3+len(args))
	fullArgs = append(fullArgs, key, strategy, threshold)
	fullArgs = append(fullArgs, args...)
	res, err := u.Send(ctx, "XTRIM", fullArgs...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}
