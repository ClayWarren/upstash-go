package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Client interface {
	Read(ctx context.Context, req Request) (any, error)
	Write(ctx context.Context, req Request) (any, error)
	Stream(ctx context.Context, req Request) (io.ReadCloser, error)
}

type Response struct {
	Result any    `json:"result"`
	Error  string `json:"error"`
}

type Request struct {
	// URL path
	Path []string
	// The body sent with the POST request
	Body any
}

type upstashClient struct {
	url          string
	edgeUrl      string
	httpClient   HTTPClient
	token        string
	enableBase64 bool
}

func New(
	// The Upstash endpoint you want to use
	url string,
	edgeUrl string,

	// Requests to the Upstash API must provide an API token.
	token string,

	enableBase64 bool,

) Client {
	httpClient := &http.Client{}

	return &upstashClient{
		url,
		edgeUrl,
		httpClient,
		token,
		enableBase64,
	}
}

// JSON marshal the body if present
func marshalBody(body any) (io.Reader, error) {
	var payload io.Reader = nil
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		payload = bytes.NewBuffer(b)
	}
	return payload, nil
}

// Perform a request and return its response
func (c *upstashClient) request(ctx context.Context, method string, path []string, body any) (any, error) {
	payload, err := marshalBody(body)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal request body: %w", err)
	}

	baseUrl := c.url
	if method == "GET" && c.edgeUrl != "" {
		baseUrl = c.edgeUrl
	}

	url := fmt.Sprintf("%s/%s", baseUrl, strings.Join(path, "/"))
	req, err := http.NewRequestWithContext(ctx, method, url, payload)
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))
	if c.enableBase64 {
		req.Header.Set("Upstash-Encoding", "base64")
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("unable to perform request: %w", err)
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var responseBody map[string]any
		err = json.NewDecoder(res.Body).Decode(&responseBody)
		if err != nil {
			return nil, fmt.Errorf("unable to decode response body of bad response: %s: %w", res.Status, err)
		}

		// Try to prettyprint the response body
		// If that is not possible we return the raw body
		pretty, err := json.MarshalIndent(responseBody, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("response returned status code %d: %+v, path: %s", res.StatusCode, responseBody, path)
		}
		return nil, fmt.Errorf("response returned status code %d: %+v, path: %s", res.StatusCode, string(pretty), path)
	}

	var rawResponse any
	err = json.NewDecoder(res.Body).Decode(&rawResponse)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal response: %w", err)
	}

	// Handle standard response: {"result": ...} or {"error": ...}
	if respMap, ok := rawResponse.(map[string]any); ok {
		if errStr, ok := respMap["error"].(string); ok && errStr != "" {
			return nil, fmt.Errorf("%s", errStr)
		}
		if res, ok := respMap["result"]; ok {
			if c.enableBase64 {
				return decodeBase64(res), nil
			}
			return res, nil
		}
		// If neither, return the map itself
		if c.enableBase64 {
			return decodeBase64(respMap), nil
		}
		return respMap, nil
	}

	// Handle pipeline/transaction response: [{"result":...}, ...]
	if respSlice, ok := rawResponse.([]any); ok {
		if c.enableBase64 {
			return decodeBase64(respSlice), nil
		}
		return respSlice, nil
	}

	if c.enableBase64 {
		return decodeBase64(rawResponse), nil
	}
	return rawResponse, nil
}

func decodeBase64(v any) any {
	switch val := v.(type) {
	case string:
		if val == "OK" {
			return val
		}
		decoded, err := base64.StdEncoding.DecodeString(val)
		if err != nil {
			return val // return raw if not base64
		}
		return string(decoded)
	case []any:
		for i, item := range val {
			val[i] = decodeBase64(item)
		}
		return val
	case map[string]any:
		for k, item := range val {
			val[k] = decodeBase64(item)
		}
		return val
	default:
		return v
	}
}

func (c *upstashClient) Read(ctx context.Context, req Request) (any, error) {
	return c.request(ctx, "GET", req.Path, nil)
}

// Call the API and unmarshal its response directly
func (c *upstashClient) Write(ctx context.Context, req Request) (any, error) {
	return c.request(ctx, "POST", req.Path, req.Body)
}

func (c *upstashClient) Stream(ctx context.Context, req Request) (io.ReadCloser, error) {
	baseUrl := c.url
	url := fmt.Sprintf("%s/%s", baseUrl, strings.Join(req.Path, "/"))

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create stream request: %w", err)
	}

	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))
	httpReq.Header.Set("Accept", "text/event-stream")

	res, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("unable to perform stream request: %w", err)
	}

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		_ = res.Body.Close()
		return nil, fmt.Errorf("stream request returned status code %d", res.StatusCode)
	}

	return res.Body, nil
}
