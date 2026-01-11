package upstash

import (
	"context"
	"fmt"
	"os"

	"github.com/claywarren/upstash-go/internal/rest"
)

// Upstash is a client for the Upstash Redis REST API.
type Upstash struct {
	client rest.Client
}

// Options provides configuration for the Upstash rest.
type Options struct {
	// Url is the Upstash endpoint you want to use.
	// Falls back to `UPSTASH_REDIS_REST_URL` environment variable.
	Url string

	// EdgeUrl is the Upstash edge url you want to use.
	// Falls back to `UPSTASH_REDIS_EDGE_URL` environment variable.
	EdgeUrl string

	// Token is the API token required for requests to the Upstash API.
	Token string

	// ReadFromEdge specifies if read requests should try to read from edge first.
	ReadFromEdge bool

	// EnableBase64 specifies if strings in the response should be base64 encoded.
	// The client will automatically decode these back to raw strings.
	EnableBase64 bool
}

// New creates a new Upstash client with the provided options.
func New(options Options) (Upstash, error) {
	if options.EdgeUrl == "" {
		options.EdgeUrl = os.Getenv("UPSTASH_REDIS_EDGE_URL")
	}

	if options.Url == "" {
		options.Url = os.Getenv("UPSTASH_REDIS_REST_URL")
	}
	if options.Token == "" {
		options.Token = os.Getenv("UPSTASH_REDIS_REST_TOKEN")
	}

	return Upstash{
		client: rest.New(options.Url, options.EdgeUrl, options.Token, options.EnableBase64),
	}, nil
}

// Send executes an arbitrary Redis command.
// It returns the raw response from the Upstash REST API.
// Use this for commands that are not yet explicitly typed in this library (e.g. HSET, LPOP).
func (u *Upstash) Send(ctx context.Context, command string, args ...any) (any, error) {
	// Construct the command body: [COMMAND, arg1, arg2, ...]
	body := make([]any, 0, 1+len(args))
	body = append(body, command)
	body = append(body, args...)

	res, err := u.client.Write(ctx, rest.Request{
		Body: body,
	})
	return res, err
}

// Pipeline represents a sequence of commands to be executed via Upstash pipeline.
type Pipeline struct {
	commands [][]any
	client   rest.Client
}

// Pipeline creates a new Pipeline.
func (u *Upstash) Pipeline() *Pipeline {
	return &Pipeline{
		commands: make([][]any, 0),
		client:   u.client,
	}
}

// Push adds a command to the pipeline.
func (p *Pipeline) Push(command string, args ...any) {
	cmd := make([]any, 0, 1+len(args))
	cmd = append(cmd, command)
	cmd = append(cmd, args...)
	p.commands = append(p.commands, cmd)
}

// Exec executes the queued commands in the pipeline.
// Returns an array of results corresponding to the commands.
func (p *Pipeline) Exec(ctx context.Context) ([]any, error) {
	// Send to /pipeline
	res, err := p.client.Write(ctx, rest.Request{
		Path: []string{"pipeline"},
		Body: p.commands,
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	// Pipeline returns an array of results
	if list, ok := res.([]any); ok {
		return list, nil
	}
	return nil, fmt.Errorf("unexpected return type for pipeline: %T", res)
}

// Multi represents a sequence of commands to be executed as a transaction.
type Multi struct {
	commands [][]any
	client   rest.Client
}

// Multi creates a new Multi (Transaction).
func (u *Upstash) Multi() *Multi {
	return &Multi{
		commands: make([][]any, 0),
		client:   u.client,
	}
}

// Push adds a command to the transaction.
func (m *Multi) Push(command string, args ...any) {
	cmd := make([]any, 0, 1+len(args))
	cmd = append(cmd, command)
	cmd = append(cmd, args...)
	m.commands = append(m.commands, cmd)
}

// Exec executes the queued commands in the transaction.
// Returns an array of results corresponding to the commands.
func (m *Multi) Exec(ctx context.Context) ([]any, error) {
	// Send to /multi-exec
	res, err := m.client.Write(ctx, rest.Request{
		Path: []string{"multi-exec"},
		Body: m.commands,
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	// Transaction returns an array of results
	if list, ok := res.([]any); ok {
		return list, nil
	}
	return nil, fmt.Errorf("unexpected return type for multi-exec: %T", res)
}

func (u *Upstash) scan(ctx context.Context, key, cursor string, options ScanOptions, command string) (ScanResult, error) {
	args := make([]any, 0)
	if key != "" {
		args = append(args, key)
	}
	args = append(args, cursor)
	if options.Match != "" {
		args = append(args, "MATCH", options.Match)
	}
	if options.Count != 0 {
		args = append(args, "COUNT", options.Count)
	}
	if options.Type != "" && command == "SCAN" {
		args = append(args, "TYPE", options.Type)
	}

	res, err := u.Send(ctx, command, args...)
	if err != nil {
		return ScanResult{}, err
	}

	list := res.([]any)
	cursorOut := fmt.Sprint(list[0])
	itemsRaw := list[1].([]any)
	items := make([]string, len(itemsRaw))
	for i, v := range itemsRaw {
		items[i] = fmt.Sprint(v)
	}

	return ScanResult{
		Cursor: cursorOut,
		Items:  items,
	}, nil
}
