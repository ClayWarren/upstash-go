package upstash

import (
	"context"
)

// SetBit sets or clears the bit at offset in the string value stored at key.
func (u *Upstash) SetBit(ctx context.Context, key string, offset int, value int) (int, error) {
	res, err := u.Send(ctx, "SETBIT", key, offset, value)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// GetBit returns the bit value at offset in the string value stored at key.
func (u *Upstash) GetBit(ctx context.Context, key string, offset int) (int, error) {
	res, err := u.Send(ctx, "GETBIT", key, offset)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// BitCount counts the number of set bits (population counting) in a string.
func (u *Upstash) BitCount(ctx context.Context, key string) (int, error) {
	res, err := u.Send(ctx, "BITCOUNT", key)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// BitOp performs a bitwise operation between multiple keys and stores the result in the destination key.
func (u *Upstash) BitOp(ctx context.Context, operation, destKey string, keys ...string) (int, error) {
	args := make([]any, 0, 2+len(keys))
	args = append(args, operation, destKey)
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "BITOP", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// BitPos returns the position of the first bit set to 1 or 0 in a string.
func (u *Upstash) BitPos(ctx context.Context, key string, bit int, startEnd ...int) (int, error) {
	args := make([]any, 0, 2+len(startEnd))
	args = append(args, key, bit)
	for _, se := range startEnd {
		args = append(args, se)
	}
	res, err := u.Send(ctx, "BITPOS", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// BitField performs arbitrary bitfield integer operations on strings.
func (u *Upstash) BitField(ctx context.Context, key string, args ...any) ([]any, error) {
	fullArgs := make([]any, 0, 1+len(args))
	fullArgs = append(fullArgs, key)
	fullArgs = append(fullArgs, args...)
	res, err := u.Send(ctx, "BITFIELD", fullArgs...)
	if err != nil {
		return nil, err
	}
	return res.([]any), nil
}

// BitFieldRO is the read-only variant of BITFIELD.
func (u *Upstash) BitFieldRO(ctx context.Context, key string, args ...any) ([]any, error) {
	fullArgs := make([]any, 0, 1+len(args))
	fullArgs = append(fullArgs, key)
	fullArgs = append(fullArgs, args...)
	res, err := u.Send(ctx, "BITFIELD_RO", fullArgs...)
	if err != nil {
		return nil, err
	}
	return res.([]any), nil
}
