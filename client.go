package upstash

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/claywarren/upstash-go/internal/rest"
)

// RetryConfig defines the retry strategy for network errors.
type RetryConfig struct {
	// Retries is the number of retry attempts. Defaults to 5.
	Retries int
	// Backoff is a function that returns the delay for a given retry attempt.
	// Defaults to exponential backoff: exp(retryCount) * 50ms.
	Backoff func(retryCount int) time.Duration
}

// Upstash is a client for the Upstash Redis REST API.
type Upstash struct {
	client rest.Client
}

// Options provides configuration for the Upstash client.
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

	// DisableTelemetry specifies if telemetry data should be sent.
	// Falls back to `UPSTASH_DISABLE_TELEMETRY` environment variable.
	DisableTelemetry bool

	// Retry defines the retry configuration.
	Retry RetryConfig

	// HTTPClient allows providing a custom http.Client.
	HTTPClient *http.Client

	// EnableAutoPipelining collects commands and sends them in a single batch.
	EnableAutoPipelining bool

	// AutoPipelineWindow is the duration to wait before flushing the auto-pipeline queue.
	// Defaults to 50ms.
	AutoPipelineWindow time.Duration

	// LatencyLogger is a callback function to log request latency.
	LatencyLogger func(command string, latency time.Duration)
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

	if !options.DisableTelemetry {
		if os.Getenv("UPSTASH_DISABLE_TELEMETRY") != "" {
			options.DisableTelemetry = true
		}
	}

	// Set defaults
	if options.Retry.Retries == 0 {
		options.Retry.Retries = 5
	}
	if options.Retry.Backoff == nil {
		options.Retry.Backoff = rest.DefaultBackoff
	}
	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{}
	}
	if options.AutoPipelineWindow == 0 {
		options.AutoPipelineWindow = 50 * time.Millisecond
	}

	u := Upstash{
		client: rest.New(options.Url, options.EdgeUrl, options.Token, options.EnableBase64, options.DisableTelemetry, options.Retry.Retries, options.Retry.Backoff, options.HTTPClient, options.LatencyLogger),
	}

	return u, nil
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
	if len(p.commands) == 0 {
		return []any{}, nil
	}
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

// Tx creates a new Multi (Transaction). Alias for Multi().
func (u *Upstash) Tx() *Multi {
	return u.Multi()
}

// Watch marks the given keys to be watched for conditional execution of a transaction.
func (u *Upstash) Watch(ctx context.Context, keys ...string) (string, error) {
	args := make([]any, 0, len(keys))
	for _, k := range keys {
		args = append(args, k)
	}
	res, err := u.Send(ctx, "WATCH", args...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// Unwatch flushes all the previously watched keys for a transaction.
func (u *Upstash) Unwatch(ctx context.Context) (string, error) {
	res, err := u.Send(ctx, "UNWATCH")
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// Discard flushes all previously queued commands in a transaction.
// Note: In REST API, this is usually client-side, but added for parity.
func (m *Multi) Discard() {
	m.commands = make([][]any, 0)
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
	if len(m.commands) == 0 {
		return []any{}, nil
	}
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
