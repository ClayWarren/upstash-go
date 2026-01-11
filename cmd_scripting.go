package upstash

import (
	"context"
)

// Eval executes a Lua script server side.
func (u *Upstash) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	cmdArgs := make([]any, 0, 2+len(keys)+len(args))
	cmdArgs = append(cmdArgs, script, len(keys))
	for _, k := range keys {
		cmdArgs = append(cmdArgs, k)
	}
	cmdArgs = append(cmdArgs, args...)
	return u.Send(ctx, "EVAL", cmdArgs...)
}

// EvalSha executes a Lua script server side by its SHA1 digest.
func (u *Upstash) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) (any, error) {
	cmdArgs := make([]any, 0, 2+len(keys)+len(args))
	cmdArgs = append(cmdArgs, sha1, len(keys))
	for _, k := range keys {
		cmdArgs = append(cmdArgs, k)
	}
	cmdArgs = append(cmdArgs, args...)
	return u.Send(ctx, "EVALSHA", cmdArgs...)
}

// ScriptLoad loads a Lua script into the scripts cache.
func (u *Upstash) ScriptLoad(ctx context.Context, script string) (string, error) {
	res, err := u.Send(ctx, "SCRIPT", "LOAD", script)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}
