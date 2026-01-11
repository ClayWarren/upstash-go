package upstash

import (
	"context"
)

// DBSize returns the number of keys in the currently-selected database.
func (u *Upstash) DBSize(ctx context.Context) (int, error) {
	res, err := u.Send(ctx, "DBSIZE")
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Info returns information and statistics about the server.
func (u *Upstash) Info(ctx context.Context, section ...string) (string, error) {
	args := make([]any, 0, len(section))
	for _, s := range section {
		args = append(args, s)
	}
	res, err := u.Send(ctx, "INFO", args...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// Time returns the current server time.
func (u *Upstash) Time(ctx context.Context) ([]string, error) {
	res, err := u.Send(ctx, "TIME")
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

// Role returns the role of the instance in the context of replication.
func (u *Upstash) Role(ctx context.Context) ([]any, error) {
	res, err := u.Send(ctx, "ROLE")
	if err != nil {
		return nil, err
	}
	return res.([]any), nil
}

// LastSave returns the Unix time stamp of the last successful save to disk.
func (u *Upstash) LastSave(ctx context.Context) (int64, error) {
	res, err := u.Send(ctx, "LASTSAVE")
	if err != nil {
		return 0, err
	}
	return int64(res.(float64)), nil
}

// Command returns information about all Redis commands.
func (u *Upstash) Command(ctx context.Context) ([]any, error) {
	res, err := u.Send(ctx, "COMMAND")
	if err != nil {
		return nil, err
	}
	return res.([]any), nil
}
