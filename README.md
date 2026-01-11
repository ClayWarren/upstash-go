# Upstash Redis Go

An HTTP/REST based Redis client for **Go**, built on top of [Upstash REST API](https://docs.upstash.com/features/restapi).

Inspired by [the official TypeScript client](https://github.com/upstash/upstash-redis).

## Why Upstash Redis?

This is the only **connectionless** (HTTP based) Redis client for Go. It is designed for environments where HTTP is preferred over TCP, such as:

- **Serverless functions** (AWS Lambda, Google Cloud Functions, etc.)
- **Edge Computing** (Cloudflare Workers, Fastly Compute@Edge)
- **WebAssembly**
- **Next.js, Jamstack** and other stateless environments.

## Installation

```bash
go get github.com/claywarren/upstash-go
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"
	
	"github.com/claywarren/upstash-go"
)

func main() {
	// Initialize the client
	// You can hardcode credentials or leave them empty to load from environment variables:
	// UPSTASH_REDIS_REST_URL
	// UPSTASH_REDIS_REST_TOKEN
	client, err := upstash.New(upstash.Options{
		Url:   "https://your-database.upstash.io",
		Token: "your-token",
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// String operations
	err = client.Set(ctx, "key", "value")
	val, err := client.Get(ctx, "key")
	fmt.Println(val) // "value"

	// Hash operations
	_, _ = client.HSet(ctx, "myhash", "field", "value")
	
	// List operations
	_, _ = client.LPush(ctx, "mylist", "element1", "element2")

	// Set operations
	_, _ = client.SAdd(ctx, "myset", "member1")

	// Sorted Set operations
	_, _ = client.ZAdd(ctx, "myzset", 1.0, "member1")
}
```

## Features

- **100% API Parity**: Every command supported by Upstash REST is implemented as a typed method.
- **Pipelining & Transactions**: Group multiple commands for atomic execution.
- **SSE Support**: Real-time `Subscribe` and `Monitor` via Go channels.
- **Context Support**: Full `context.Context` support for cancellations and timeouts.
- **Automatic Retries**: Built-in retry logic for network-level failures.
- **Binary Safe**: Optional Base64 encoding for binary data.

## Configuration

```go
type Options struct {
	Url              string // REST URL
	Token            string // REST Token
	EdgeUrl          string // Optional: Global Edge URL
	ReadFromEdge     bool   // Try reading from edge first
	EnableBase64     bool   // Enable automatic base64 encoding/decoding
	DisableTelemetry bool   // Disable anonymous usage tracking
}
```

### Telemetry

This library sends anonymous telemetry data (SDK version, Platform) to help Upstash improve the experience. You can opt out in two ways:

1. Set the `UPSTASH_DISABLE_TELEMETRY` environment variable to any non-empty value.
2. Pass `DisableTelemetry: true` in the `upstash.Options` during initialization.

## Development

```bash
make check # Run format, build, test, and lint
make test  # Run unit tests with coverage
```

## License

MIT - Copyright (c) 2026 Clay Warren
