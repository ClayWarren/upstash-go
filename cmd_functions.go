package upstash

import (
	"context"
)

// FCall calls a function.
func (u *Upstash) FCall(ctx context.Context, function string, keys []string, args ...any) (any, error) {
	cmdArgs := make([]any, 0, 2+len(keys)+len(args))
	cmdArgs = append(cmdArgs, function, len(keys))
	for _, k := range keys {
		cmdArgs = append(cmdArgs, k)
	}
	cmdArgs = append(cmdArgs, args...)
	return u.Send(ctx, "FCALL", cmdArgs...)
}

// FCallRO calls a read-only function.
func (u *Upstash) FCallRO(ctx context.Context, function string, keys []string, args ...any) (any, error) {
	cmdArgs := make([]any, 0, 2+len(keys)+len(args))
	cmdArgs = append(cmdArgs, function, len(keys))
	for _, k := range keys {
		cmdArgs = append(cmdArgs, k)
	}
	cmdArgs = append(cmdArgs, args...)
	return u.Send(ctx, "FCALL_RO", cmdArgs...)
}

// FunctionLoad loads a library of functions.
func (u *Upstash) FunctionLoad(ctx context.Context, payload string, replace bool) (string, error) {
	args := []any{"LOAD"}
	if replace {
		args = append(args, "REPLACE")
	}
	args = append(args, payload)
	res, err := u.Send(ctx, "FUNCTION", args...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// FunctionList returns information about the libraries and functions.
func (u *Upstash) FunctionList(ctx context.Context, libraryName ...string) ([]any, error) {
	args := []any{"LIST"}
	if len(libraryName) > 0 {
		args = append(args, "LIBRARYNAME", libraryName[0])
	}
	res, err := u.Send(ctx, "FUNCTION", args...)
	if err != nil {
		return nil, err
	}
	return res.([]any), nil
}

// FunctionDelete deletes a library and all its functions.
func (u *Upstash) FunctionDelete(ctx context.Context, libraryName string) (string, error) {
	res, err := u.Send(ctx, "FUNCTION", "DELETE", libraryName)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// FunctionFlush deletes all libraries and functions.
func (u *Upstash) FunctionFlush(ctx context.Context) (string, error) {
	res, err := u.Send(ctx, "FUNCTION", "FLUSH")
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// FunctionStats returns information about the current function execution.
func (u *Upstash) FunctionStats(ctx context.Context) (any, error) {
	return u.Send(ctx, "FUNCTION", "STATS")
}
