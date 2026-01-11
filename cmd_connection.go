package upstash

import (
	"context"
)

// Ping returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk.
func (u *Upstash) Ping(ctx context.Context, message ...string) (string, error) {
	args := make([]any, 0, len(message))
	for _, m := range message {
		args = append(args, m)
	}
	res, err := u.Send(ctx, "PING", args...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// Echo returns message.
func (u *Upstash) Echo(ctx context.Context, message string) (string, error) {
	res, err := u.Send(ctx, "ECHO", message)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}
