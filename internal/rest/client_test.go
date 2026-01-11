package rest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/claywarren/upstash-go/internal/rest"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	c := rest.New("http://example.com", "http://edge.example.com", "token", false)
	require.NotNil(t, c)
}

func TestRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET", r.Method)
		require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
		require.Equal(t, "/get/foo", r.URL.Path)

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": "bar",
		})
	}))
	defer server.Close()

	c := rest.New(server.URL, "", "token", false)
	res, err := c.Read(context.Background(), rest.Request{
		Path: []string{"get", "foo"},
	})
	require.NoError(t, err)
	require.Equal(t, "bar", res)
}

func TestWrite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
		require.Equal(t, "/set/foo/bar", r.URL.Path)

		// Check body
		var body any
		err := json.NewDecoder(r.Body).Decode(&body)
		require.NoError(t, err)
		require.Equal(t, "body-content", body)

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": "OK",
		})
	}))
	defer server.Close()

	c := rest.New(server.URL, "", "token", false)
	res, err := c.Write(context.Background(), rest.Request{
		Path: []string{"set", "foo", "bar"},
		Body: "body-content",
	})
	require.NoError(t, err)
	require.Equal(t, "OK", res)
}

func TestEdgeUrl(t *testing.T) {
	edgeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "GET", r.Method)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": "from-edge",
		})
	}))
	defer edgeServer.Close()

	restServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Should not hit REST server for GET when EdgeURL is set")
	}))
	defer restServer.Close()

	c := rest.New(restServer.URL, edgeServer.URL, "token", false)
	res, err := c.Read(context.Background(), rest.Request{
		Path: []string{"get", "foo"},
	})
	require.NoError(t, err)
	require.Equal(t, "from-edge", res)
}

func TestApiError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": "ERR syntax error",
		})
	}))
	defer server.Close()

	c := rest.New(server.URL, "", "token", false)
	_, err := c.Read(context.Background(), rest.Request{Path: []string{"get"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ERR syntax error")
}

func TestServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": "Internal Server Error",
		})
	}))
	defer server.Close()

	c := rest.New(server.URL, "", "token", false)
	_, err := c.Read(context.Background(), rest.Request{Path: []string{"get"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "response returned status code 500")
}

func TestResponseErrorField(t *testing.T) {
	// Tests the case where status is 200 but the JSON body contains an "error" field
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": "ERR logical error",
		})
	}))
	defer server.Close()

	c := rest.New(server.URL, "", "token", false)
	_, err := c.Read(context.Background(), rest.Request{Path: []string{"get"}})
	require.Error(t, err)
	require.Equal(t, "ERR logical error", err.Error())
}

func TestMarshalError(t *testing.T) {
	c := rest.New("http://example.com", "", "token", false)
	// Pass a channel which cannot be marshaled to JSON
	_, err := c.Write(context.Background(), rest.Request{
		Body: make(chan int),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unable to marshal request body")
}

func TestBase64Decoding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "base64", r.Header.Get("Upstash-Encoding"))
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"scalar": "YmFy",             // "bar"
				"list":   []any{"YmFy", 123}, // "bar", 123
				"ok":     "OK",               // should not be decoded
			},
		})
	}))
	defer server.Close()

	c := rest.New(server.URL, "", "token", true)
	res, err := c.Read(context.Background(), rest.Request{})
	require.NoError(t, err)

	resMap := res.(map[string]any)
	require.Equal(t, "bar", resMap["scalar"])
	require.Equal(t, "bar", resMap["list"].([]any)[0])
	require.Equal(t, float64(123), resMap["list"].([]any)[1])
	require.Equal(t, "OK", resMap["ok"])
}

func TestStream(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "text/event-stream", r.Header.Get("Accept"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: hello\n\n"))
	}))
	defer server.Close()

	c := rest.New(server.URL, "", "token", false)
	stream, err := c.Stream(context.Background(), rest.Request{Path: []string{"sub"}})
	require.NoError(t, err)
	require.NotNil(t, stream)

	buf := make([]byte, 100)
	n, _ := stream.Read(buf)
	require.Contains(t, string(buf[:n]), "data: hello")
	_ = stream.Close()
}
