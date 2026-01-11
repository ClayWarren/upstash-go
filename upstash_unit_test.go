package upstash_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/claywarren/upstash-go"
	"github.com/stretchr/testify/require"
)

type mockHandler struct {
	method       string
	expectedBody []any
	response     any
	status       int
}

func setupMockServer(t *testing.T, handlers []mockHandler) (*upstash.Upstash, func()) {
	step := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if step >= len(handlers) {
			t.Errorf("Unexpected request number %d", step)
			return
		}
		h := handlers[step]

		// Verify Method
		require.Equal(t, h.method, r.Method)

		// Verify Authorization
		require.Equal(t, "Bearer mock-token", r.Header.Get("Authorization"))

		// Verify Body if POST
		if r.Method == "POST" {
			var body []any
			_ = json.NewDecoder(r.Body).Decode(&body)
			require.Equal(t, h.expectedBody, body)
		}

		// Send Response
		w.WriteHeader(h.status)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": h.response,
		})
		step++
	}))

	u, err := upstash.New(upstash.Options{
		Url:   server.URL,
		Token: "mock-token",
	})
	require.NoError(t, err)

	return &u, server.Close
}

func TestUnitKeys(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "GET",
			expectedBody: []any{"keys", "*"},
			response:     []any{"k1", "k2"},
			status:       200,
		},
	})
	defer close()

	keys, err := u.Keys(context.Background(), "*")
	require.NoError(t, err)
	require.Equal(t, []string{"k1", "k2"}, keys)
}

func TestUnitAppend(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"append", "k", "v"},
			response:     float64(5),
			status:       200,
		},
	})
	defer close()

	val, err := u.Append(context.Background(), "k", "v")
	require.NoError(t, err)
	require.Equal(t, 5, val)
}

func TestUnitDecr(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"decr", "k"},
			response:     float64(9),
			status:       200,
		},
	})
	defer close()

	val, err := u.Decr(context.Background(), "k")
	require.NoError(t, err)
	require.Equal(t, 9, val)
}

func TestUnitDecrBy(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"decrby", "k", "2"},
			response:     float64(8),
			status:       200,
		},
	})
	defer close()

	val, err := u.DecrBy(context.Background(), "k", 2)
	require.NoError(t, err)
	require.Equal(t, 8, val)
}

func TestUnitGet(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:   "GET",
			response: "bar",
			status:   200,
		},
	})
	defer close()

	val, err := u.Get(context.Background(), "foo")
	require.NoError(t, err)
	require.Equal(t, "bar", val)
}

func TestUnitGetEx(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"getex", "k", "ex", "60"},
			response:     "v",
			status:       200,
		},
	})
	defer close()

	val, err := u.GetEx(context.Background(), "k", upstash.GetEXOptions{EX: 60})
	require.NoError(t, err)
	require.Equal(t, "v", val)
}

func TestUnitGetRange(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:   "GET",
			response: "lo",
			status:   200,
		},
	})
	defer close()

	val, err := u.GetRange(context.Background(), "hello", 2, 3)
	require.NoError(t, err)
	require.Equal(t, "lo", val)
}

func TestUnitGetSet(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"getset", "k", "new"},
			response:     "old",
			status:       200,
		},
	})
	defer close()

	val, err := u.GetSet(context.Background(), "k", "new")
	require.NoError(t, err)
	require.Equal(t, "old", val)
}

func TestUnitIncr(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"incr", "counter"},
			response:     float64(5),
			status:       200,
		},
	})
	defer close()

	val, err := u.Incr(context.Background(), "counter")
	require.NoError(t, err)
	require.Equal(t, 5, val)
}

func TestUnitIncrBy(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"incrby", "counter", "2"},
			response:     float64(7),
			status:       200,
		},
	})
	defer close()

	val, err := u.IncrBy(context.Background(), "counter", 2)
	require.NoError(t, err)
	require.Equal(t, 7, val)
}

func TestUnitIncrByFloat(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"incrbyfloat", "k", "1.500000"},
			response:     "2.5", // Redis returns string for floats
			status:       200,
		},
	})
	defer close()

	val, err := u.IncrByFloat(context.Background(), "k", 1.5)
	require.NoError(t, err)
	require.Equal(t, 2.5, val)
}

func TestUnitMGet(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:   "GET",
			response: []any{"v1", "v2"},
			status:   200,
		},
	})
	defer close()

	vals, err := u.MGet(context.Background(), []string{"k1", "k2"})
	require.NoError(t, err)
	require.Equal(t, []string{"v1", "v2"}, vals)
}

func TestUnitMSet(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"mset", "k1", "v1", "k2", "v2"},
			response:     "OK",
			status:       200,
		},
	})
	defer close()

	err := u.MSet(context.Background(), []upstash.KV{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}})
	require.NoError(t, err)
}

func TestUnitMSetNX(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"msetnx", "k1", "v1"},
			response:     float64(1),
			status:       200,
		},
	})
	defer close()

	val, err := u.MSetNX(context.Background(), []upstash.KV{{Key: "k1", Value: "v1"}})
	require.NoError(t, err)
	require.Equal(t, 1, val)
}

func TestUnitPSetEX(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"psetex", "k", "1000", "v"},
			response:     "OK",
			status:       200,
		},
	})
	defer close()

	err := u.PSetEX(context.Background(), "k", 1000, "v")
	require.NoError(t, err)
}

func TestUnitSet(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"set", "foo", "bar"},
			response:     "OK",
			status:       200,
		},
	})
	defer close()

	err := u.Set(context.Background(), "foo", "bar")
	require.NoError(t, err)
}

func TestUnitSetWithOptions(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"set", "k", "v", "ex", "10", "nx"},
			response:     "OK",
			status:       200,
		},
	})
	defer close()

	err := u.SetWithOptions(context.Background(), "k", "v", upstash.SetOptions{EX: 10, NX: true})
	require.NoError(t, err)
}

func TestUnitSetEX(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"setex", "k", "10", "v"},
			response:     "OK",
			status:       200,
		},
	})
	defer close()

	err := u.SetEX(context.Background(), "k", 10, "v")
	require.NoError(t, err)
}

func TestUnitSetNX(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"setnx", "k", "v"},
			response:     float64(1),
			status:       200,
		},
	})
	defer close()

	val, err := u.SetNX(context.Background(), "k", "v")
	require.NoError(t, err)
	require.Equal(t, 1, val)
}

func TestUnitSetRange(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"setrange", "k", "2", "v"},
			response:     float64(5),
			status:       200,
		},
	})
	defer close()

	err := u.SetRange(context.Background(), "k", 2, "v")
	require.NoError(t, err)
}

func TestUnitStrLen(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:   "GET",
			response: float64(10),
			status:   200,
		},
	})
	defer close()

	val, err := u.StrLen(context.Background(), "k")
	require.NoError(t, err)
	require.Equal(t, 10, val)
}

func TestUnitFlushAll(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"flushall"},
			response:     "OK",
			status:       200,
		},
	})
	defer close()

	err := u.FlushAll(context.Background())
	require.NoError(t, err)
}

func TestUnitError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": "ERR wrong number of arguments",
		})
	}))
	defer server.Close()

	u, err := upstash.New(upstash.Options{
		Url:   server.URL,
		Token: "mock-token",
	})
	require.NoError(t, err)

	_, err = u.Get(context.Background(), "foo")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ERR wrong number of arguments")
}

func TestUnitClientErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "mock error"})
	}))
	defer server.Close()

	u, _ := upstash.New(upstash.Options{Url: server.URL, Token: "t"})
	ctx := context.Background()

	t.Run("Append", func(t *testing.T) { _, err := u.Append(ctx, "k", "v"); require.Error(t, err) })
	t.Run("Decr", func(t *testing.T) { _, err := u.Decr(ctx, "k"); require.Error(t, err) })
	t.Run("DecrBy", func(t *testing.T) { _, err := u.DecrBy(ctx, "k", 1); require.Error(t, err) })
	t.Run("GetRange", func(t *testing.T) { _, err := u.GetRange(ctx, "k", 0, 1); require.Error(t, err) })
	t.Run("GetSet", func(t *testing.T) { _, err := u.GetSet(ctx, "k", "v"); require.Error(t, err) })
	t.Run("Incr", func(t *testing.T) { _, err := u.Incr(ctx, "k"); require.Error(t, err) })
	t.Run("IncrBy", func(t *testing.T) { _, err := u.IncrBy(ctx, "k", 1); require.Error(t, err) })
	t.Run("IncrByFloat", func(t *testing.T) { _, err := u.IncrByFloat(ctx, "k", 1.1); require.Error(t, err) })
	t.Run("MGet", func(t *testing.T) { _, err := u.MGet(ctx, []string{"k"}); require.Error(t, err) })
	t.Run("MSetNX", func(t *testing.T) {
		_, err := u.MSetNX(ctx, []upstash.KV{{Key: "k", Value: "v"}})
		require.Error(t, err)
	})
	t.Run("SetNX", func(t *testing.T) { _, err := u.SetNX(ctx, "k", "v"); require.Error(t, err) })
	t.Run("StrLen", func(t *testing.T) { _, err := u.StrLen(ctx, "k"); require.Error(t, err) })
}

func TestUnitSetWithOptionsErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "mock error"})
	}))
	defer server.Close()

	u, _ := upstash.New(upstash.Options{Url: server.URL, Token: "t"})

	err := u.SetWithOptions(context.Background(), "k", "v", upstash.SetOptions{EX: 10})
	require.Error(t, err)
	require.Contains(t, err.Error(), "error [set k v ex 10]")
}

func TestUnitKeysError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	u, _ := upstash.New(upstash.Options{Url: server.URL, Token: "t"})
	_, err := u.Keys(context.Background(), "*")
	require.Error(t, err)
}

func TestUnitKeysUnexpectedType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": float64(123), // Unexpected type for Keys
		})
	}))
	defer server.Close()

	u, _ := upstash.New(upstash.Options{Url: server.URL, Token: "t"})
	_, err := u.Keys(context.Background(), "*")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected return type for keys")
}
