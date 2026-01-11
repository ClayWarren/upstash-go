package upstash

import (
	"bufio"
	"context"
	"io"
	"strings"

	"github.com/claywarren/upstash-go/internal/rest"
)

// Publish posts a message to the given channel.
func (u *Upstash) Publish(ctx context.Context, channel, message string) (int, error) {
	res, err := u.Send(ctx, "PUBLISH", channel, message)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// Subscribe subscribes to a channel and returns a channel of messages.
func (u *Upstash) Subscribe(ctx context.Context, channel string) (<-chan string, error) {
	stream, err := u.client.Stream(ctx, rest.Request{
		Path: []string{"subscribe", channel},
	})
	if err != nil {
		return nil, err
	}

	out := make(chan string)
	go u.streamReader(ctx, stream, out)
	return out, nil
}

// Monitor monitors all commands hitting the database in real-time.
func (u *Upstash) Monitor(ctx context.Context) (<-chan string, error) {
	stream, err := u.client.Stream(ctx, rest.Request{
		Path: []string{"monitor"},
	})
	if err != nil {
		return nil, err
	}

	out := make(chan string)
	go u.streamReader(ctx, stream, out)
	return out, nil
}

func (u *Upstash) streamReader(ctx context.Context, stream io.ReadCloser, out chan<- string) {
	defer func() {
		_ = stream.Close()
	}()
	defer close(out)

	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			msg := strings.TrimPrefix(line, "data: ")
			// Upstash might wrap the data in quotes if it's a string from JSON
			if strings.HasPrefix(msg, "\"") && strings.HasSuffix(msg, "\"") && len(msg) >= 2 {
				msg = msg[1 : len(msg)-1]
			}
			select {
			case out <- msg:
			case <-ctx.Done():
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
