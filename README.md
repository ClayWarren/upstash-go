# Upstash Redis Go

An HTTP/REST based Redis client built on top of [Upstash REST API](https://docs.upstash.com/features/restapi).

Inspired by [the official TypeScript client](https://github.com/upstash/upstash-redis).

See [the list of APIs](https://docs.upstash.com/features/restapi#rest---redis-api-compatibility) supported.

[![codecov](https://codecov.io/gh/claywarren/upstash-go/branch/main/graph/badge.svg?token=BCNI6L3TRT)](https://codecov.io/gh/claywarren/upstash-go)

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
	// UPSTASH_REDIS_EDGE_URL (optional)
	options := upstash.Options{
		Url:     "", 
		Token:   "", 
	}

	client, err := upstash.New(options)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Set a key
	err = client.Set(ctx, "foo", "bar")
	if err != nil {
		log.Fatal(err)
	}

	// Get a key
	value, err := client.Get(ctx, "foo")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(value)
	// -> "bar"
}
```

## Configuration

The `upstash.New` function accepts an `Options` struct for configuration:

```go
type Options struct {
	// The Upstash endpoint you want to use
	// Falls back to `UPSTASH_REDIS_REST_URL` environment variable.
	Url string

	// The Upstash edge url you want to use (optional)
	// Falls back to `UPSTASH_REDIS_EDGE_URL` environment variable.
	EdgeUrl string

	// Requests to the Upstash API must provide an API token.
	// Falls back to `UPSTASH_REDIS_REST_TOKEN` environment variable.
	Token string

	// If true, read requests will try to read from edge first
	ReadFromEdge bool
}
```

### Environment Variables

You can configure the client using the following environment variables instead of passing them explicitly:

- `UPSTASH_REDIS_REST_URL`: Your Upstash Redis REST URL.
- `UPSTASH_REDIS_REST_TOKEN`: Your Upstash Redis REST Token.
- `UPSTASH_REDIS_EDGE_URL`: (Optional) Your Upstash Global Database URL for lower latency reads.