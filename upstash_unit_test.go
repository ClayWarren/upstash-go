package upstash_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/claywarren/upstash-go"
	"github.com/stretchr/testify/require"
)

type mockHandler struct {
	method       string
	path         string // Optional: check path
	expectedBody any    // changed from []any to allow verifying 2D arrays for pipeline
	response     any
	rawResponse  bool // if true, do not wrap in {"result":...}
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

		// Verify Path (if specified)
		if h.path != "" {
			require.Equal(t, h.path, r.URL.Path)
		}

		// Verify Body if POST
		if r.Method == "POST" {
			var body any
			_ = json.NewDecoder(r.Body).Decode(&body)

			// We need to compare expectedBody and body.
			// Since json decoding produces generic maps/slices, we rely on testify.Equal
			require.Equal(t, h.expectedBody, body)
		}

		// Send Response
		w.WriteHeader(h.status)
		if h.rawResponse {
			_ = json.NewEncoder(w).Encode(h.response)
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": h.response,
			})
		}
		step++
	}))

	u, err := upstash.New(upstash.Options{
		Url:   server.URL,
		Token: "mock-token",
	})
	require.NoError(t, err)

	return &u, server.Close
}

func TestUnitSend(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"HSET", "myhash", "field1", "value1"},
			response:     float64(1),
			status:       200,
		},
	})
	defer close()

	val, err := u.Send(context.Background(), "HSET", "myhash", "field1", "value1")
	require.NoError(t, err)
	require.Equal(t, 1.0, val)
}

func TestUnitPipeline(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method: "POST",
			path:   "/pipeline",
			expectedBody: []any{
				[]any{"SET", "k", "v"},
				[]any{"GET", "k"},
			},
			response: []any{
				map[string]any{"result": "OK"},
				map[string]any{"result": "v"},
			},
			rawResponse: true,
			status:      200,
		},
	})
	defer close()

	pipe := u.Pipeline()
	pipe.Push("SET", "k", "v")
	pipe.Push("GET", "k")

	res, err := pipe.Exec(context.Background())
	require.NoError(t, err)
	require.Len(t, res, 2)
	// Response is generic map from JSON
	require.Equal(t, "OK", res[0].(map[string]any)["result"])
	require.Equal(t, "v", res[1].(map[string]any)["result"])
}

func TestUnitMulti(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method: "POST",
			path:   "/multi-exec",
			expectedBody: []any{
				[]any{"SET", "k", "v"},
				[]any{"INCR", "c"},
			},
			response: []any{
				map[string]any{"result": "OK"},
				map[string]any{"result": float64(1)},
			},
			rawResponse: true,
			status:      200,
		},
	})
	defer close()

	tx := u.Multi()
	tx.Push("SET", "k", "v")
	tx.Push("INCR", "c")

	res, err := tx.Exec(context.Background())
	require.NoError(t, err)
	require.Len(t, res, 2)
	require.Equal(t, float64(1), res[1].(map[string]any)["result"])
}

func TestUnitKeys(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "GET",
			path:         "/keys/*",
			expectedBody: nil,
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

func TestUnitBase64(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "base64", r.Header.Get("Upstash-Encoding"))
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": "YmFy", // "bar"
		})
	}))
	defer server.Close()

	u, _ := upstash.New(upstash.Options{
		Url:          server.URL,
		Token:        "t",
		EnableBase64: true,
	})

	val, err := u.Get(context.Background(), "foo")
	require.NoError(t, err)
	require.Equal(t, "bar", val)
}

func TestUnitHashMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"HSET", "h", "f", "v"},
			response:     float64(1),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"HGET", "h", "f"},
			response:     "v",
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"HGETALL", "h"},
			response:     []any{"f1", "v1", "f2", "v2"},
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"HDEL", "h", "f1", "f2"},
			response:     float64(2),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"HLEN", "h"},
			response:     float64(2),
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()

	res, err := u.HSet(ctx, "h", "f", "v")
	require.NoError(t, err)
	require.Equal(t, 1, res)

	val, err := u.HGet(ctx, "h", "f")
	require.NoError(t, err)
	require.Equal(t, "v", val)

	all, err := u.HGetAll(ctx, "h")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"f1": "v1", "f2": "v2"}, all)

	deleted, err := u.HDel(ctx, "h", "f1", "f2")
	require.NoError(t, err)
	require.Equal(t, 2, deleted)

	length, err := u.HLen(ctx, "h")
	require.NoError(t, err)
	require.Equal(t, 2, length)
}

func TestUnitListMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"LPUSH", "l", "v1", "v2"},
			response:     float64(2),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"RPUSH", "l", "v3"},
			response:     float64(3),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"LPOP", "l"},
			response:     "v2",
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"RPOP", "l"},
			response:     "v3",
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"LLEN", "l"},
			response:     float64(1),
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()

	res, err := u.LPush(ctx, "l", "v1", "v2")
	require.NoError(t, err)
	require.Equal(t, 2, res)

	res, err = u.RPush(ctx, "l", "v3")
	require.NoError(t, err)
	require.Equal(t, 3, res)

	val, err := u.LPop(ctx, "l")
	require.NoError(t, err)
	require.Equal(t, "v2", val)

	val, err = u.RPop(ctx, "l")
	require.NoError(t, err)
	require.Equal(t, "v3", val)

	length, err := u.LLen(ctx, "l")
	require.NoError(t, err)
	require.Equal(t, 1, length)
}

func TestUnitScanMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"SCAN", "0", "MATCH", "user:*", "COUNT", float64(10), "TYPE", "string"},
			response:     []any{"123", []any{"user:1", "user:2"}},
			status:       200,
		},
	})
	defer close()

	res, err := u.Scan(context.Background(), "0", upstash.ScanOptions{
		Match: "user:*",
		Count: 10,
		Type:  "string",
	})
	require.NoError(t, err)
	require.Equal(t, "123", res.Cursor)
	require.Equal(t, []string{"user:1", "user:2"}, res.Items)

	// HScan test
	u2, close2 := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"HSCAN", "myhash", "0"},
			response:     []any{"0", []any{"f1", "v1"}},
			status:       200,
		},
	})
	defer close2()
	hres, err := u2.HScan(context.Background(), "myhash", "0", upstash.ScanOptions{})
	require.NoError(t, err)
	require.Equal(t, "0", hres.Cursor)
	require.Equal(t, []string{"f1", "v1"}, hres.Items)
}

func TestUnitHyperLogLog(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"PFADD", "hll", "a", "b"},
			response:     float64(1),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"PFCOUNT", "hll"},
			response:     float64(2),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"PFMERGE", "dest", "s1", "s2"},
			response:     "OK",
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()
	res, err := u.PFAdd(ctx, "hll", "a", "b")
	require.NoError(t, err)
	require.Equal(t, 1, res)

	count, err := u.PFCount(ctx, "hll")
	require.NoError(t, err)
	require.Equal(t, 2, count)

	err = u.PFMerge(ctx, "dest", "s1", "s2")
	require.NoError(t, err)
}

func TestUnitBitmaps(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"SETBIT", "b", float64(10), float64(1)},
			response:     float64(0),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"GETBIT", "b", float64(10)},
			response:     float64(1),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"BITCOUNT", "b"},
			response:     float64(5),
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()
	res, err := u.SetBit(ctx, "b", 10, 1)
	require.NoError(t, err)
	require.Equal(t, 0, res)

	bit, err := u.GetBit(ctx, "b", 10)
	require.NoError(t, err)
	require.Equal(t, 1, bit)

	bc, err := u.BitCount(ctx, "b")
	require.NoError(t, err)
	require.Equal(t, 5, bc)
}

func TestUnitCompletionist_Group1(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{method: "POST", expectedBody: []any{"COPY", "s", "d"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"DUMP", "k"}, response: "dump", status: 200},
		{method: "POST", expectedBody: []any{"EXPIREAT", "k", float64(12345)}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"PERSIST", "k"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"RANDOMKEY"}, response: "rk", status: 200},
		{method: "POST", expectedBody: []any{"RENAME", "k1", "k2"}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"TOUCH", "k"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"TYPE", "k"}, response: "string", status: 200},
		{method: "POST", expectedBody: []any{"UNLINK", "k"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"HEXISTS", "h", "f"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"HINCRBYFLOAT", "h", "f", 1.1}, response: "2.2", status: 200},
		{method: "POST", expectedBody: []any{"HKEYS", "h"}, response: []any{"f1"}, status: 200},
		{method: "POST", expectedBody: []any{"HMSET", "h", "f1", "v1"}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"LINDEX", "l", float64(0)}, response: "v", status: 200},
		{method: "POST", expectedBody: []any{"LPUSHX", "l", "v"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"LREM", "l", float64(1), "v"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"LTRIM", "l", float64(0), float64(1)}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"SDIFF", "k1", "k2"}, response: []any{"v"}, status: 200},
		{method: "POST", expectedBody: []any{"SINTER", "k1", "k2"}, response: []any{"v"}, status: 200},
		{method: "POST", expectedBody: []any{"SMISMEMBER", "s", "m1"}, response: []any{float64(1)}, status: 200},
		{method: "POST", expectedBody: []any{"ZCOUNT", "zs", float64(0), float64(10)}, response: float64(5), status: 200},
		{method: "POST", expectedBody: []any{"ZREMRANGEBYRANK", "zs", float64(0), float64(1)}, response: float64(2), status: 200},
	})
	defer close()

	ctx := context.Background()
	_, _ = u.Copy(ctx, "s", "d")
	_, _ = u.Dump(ctx, "k")
	_, _ = u.ExpireAt(ctx, "k", 12345)
	_, _ = u.Persist(ctx, "k")
	_, _ = u.RandomKey(ctx)
	_ = u.Rename(ctx, "k1", "k2")
	_, _ = u.Touch(ctx, "k")
	_, _ = u.Type(ctx, "k")
	_, _ = u.Unlink(ctx, "k")
	_, _ = u.HExists(ctx, "h", "f")
	_, _ = u.HIncrByFloat(ctx, "h", "f", 1.1)
	_, _ = u.HKeys(ctx, "h")
	_, _ = u.HMSet(ctx, "h", map[string]string{"f1": "v1"})
	_, _ = u.LIndex(ctx, "l", 0)
	_, _ = u.LPushX(ctx, "l", "v")
	_, _ = u.LRem(ctx, "l", 1, "v")
	_, _ = u.LTrim(ctx, "l", 0, 1)
	_, _ = u.SDiff(ctx, "k1", "k2")
	_, _ = u.SInter(ctx, "k1", "k2")
	_, _ = u.SMIsMember(ctx, "s", "m1")
	_, _ = u.ZCount(ctx, "zs", 0, 10)
	_, _ = u.ZRemRangeByRank(ctx, "zs", 0, 1)
}

func TestUnitCompletionist_Group2(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{method: "POST", expectedBody: []any{"GEOHASH", "sicily", "Palermo"}, response: []any{"sqc8bzn0u10"}, status: 200},
		{method: "POST", expectedBody: []any{"GEORADIUSBYMEMBER", "sicily", "Palermo", float64(100), "km"}, response: []any{"Palermo"}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.CLEAR", "doc", "$"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"JSON.OBJKEYS", "doc", "$"}, response: []any{"a"}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.TOGGLE", "doc", "$.bool"}, response: []any{true}, status: 200},
		{method: "POST", expectedBody: []any{"XACK", "s", "g", "id1"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"XTRIM", "s", "MAXLEN", float64(10)}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"DBSIZE"}, response: float64(100), status: 200},
		{method: "POST", expectedBody: []any{"TIME"}, response: []any{"1518390000", "123456"}, status: 200},
		{method: "POST", expectedBody: []any{"LASTSAVE"}, response: float64(1518390000), status: 200},
	})
	defer close()

	ctx := context.Background()
	_, _ = u.GeoHash(ctx, "sicily", "Palermo")
	_, _ = u.GeoRadiusByMember(ctx, "sicily", "Palermo", 100, "km")
	_, _ = u.JsonClear(ctx, "doc", "$")
	_, _ = u.JsonObjKeys(ctx, "doc", "$")
	_, _ = u.JsonToggle(ctx, "doc", "$.bool")
	_, _ = u.XAck(ctx, "s", "g", "id1")
	_, _ = u.XTrim(ctx, "s", "MAXLEN", 10)
	_, _ = u.DBSize(ctx)
	_, _ = u.Time(ctx)
	_, _ = u.LastSave(ctx)
}

func TestUnitCompletionist_Group3(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{method: "POST", expectedBody: []any{"MOVE", "k", float64(1)}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"RESTORE", "k", float64(0), "v"}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"ZINTER", float64(2), "k1", "k2"}, response: []any{"v"}, status: 200},
		{method: "POST", expectedBody: []any{"ZMPOP", float64(1), "k", "MIN"}, response: []any{"v"}, status: 200},
		{method: "POST", expectedBody: []any{"COMMAND"}, response: []any{"set", "get"}, status: 200},
		{method: "POST", expectedBody: []any{"FCALL", "f", float64(1), "k", "a"}, response: "res", status: 200},
		{method: "POST", expectedBody: []any{"FUNCTION", "LOAD", "p"}, response: "L", status: 200},
		{method: "POST", expectedBody: []any{"FUNCTION", "LIST"}, response: []any{"L"}, status: 200},
	})
	defer close()

	ctx := context.Background()
	_, _ = u.Move(ctx, "k", 1)
	_, _ = u.Restore(ctx, "k", 0, "v", false)
	_, _ = u.ZInter(ctx, 2, []string{"k1", "k2"})
	_, _ = u.ZMPop(ctx, 1, []string{"k"}, "MIN")
	_, _ = u.Command(ctx)
	_, _ = u.FCall(ctx, "f", []string{"k"}, "a")
	_, _ = u.FunctionLoad(ctx, "p", false)
	_, _ = u.FunctionList(ctx)
}

func TestUnitCoverageFinalPush(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{method: "POST", expectedBody: []any{"HSETNX", "h", "f", "v"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"HSTRLEN", "h", "f"}, response: float64(5), status: 200},
		{method: "POST", expectedBody: []any{"HVALS", "h"}, response: []any{"v1"}, status: 200},
		{method: "POST", expectedBody: []any{"LINSERT", "l", "BEFORE", "p", "e"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"LMOVE", "s", "d", "LEFT", "RIGHT"}, response: "v", status: 200},
		{method: "POST", expectedBody: []any{"LPOS", "l", "e"}, response: float64(0), status: 200},
		{method: "POST", expectedBody: []any{"LSET", "l", float64(0), "v"}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"RPOPLPUSH", "s", "d"}, response: "v", status: 200},
		{method: "POST", expectedBody: []any{"SDIFFSTORE", "d", "k1", "k2"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"SINTERSTORE", "d", "k1", "k2"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"SMOVE", "s", "d", "m"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"SUNION", "k1", "k2"}, response: []any{"v"}, status: 200},
		{method: "POST", expectedBody: []any{"SUNIONSTORE", "d", "k1", "k2"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"ZDIFF", float64(2), "k1", "k2"}, response: []any{"v"}, status: 200},
		{method: "POST", expectedBody: []any{"ZLEXCOUNT", "zs", "a", "b"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"ZMPOP", float64(1), "k", "MAX", "COUNT", float64(1)}, response: []any{"v"}, status: 200},
		{method: "POST", expectedBody: []any{"ZREMRANGEBYLEX", "zs", "a", "b"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"ZREMRANGEBYSCORE", "zs", float64(0), float64(10)}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"ZREVRANGE", "zs", float64(0), float64(-1)}, response: []any{"v"}, status: 200},
		{method: "POST", expectedBody: []any{"ZREVRANK", "zs", "m"}, response: float64(0), status: 200},
		{method: "POST", expectedBody: []any{"ZUNIONSTORE", "d", float64(2), "k1", "k2"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"ZREVRANGEBYLEX", "zs", "max", "min", "LIMIT", float64(0), float64(1)}, response: []any{"v"}, status: 200},
		{method: "POST", expectedBody: []any{"ZREVRANGEBYSCORE", "zs", "max", "min", "LIMIT", float64(0), float64(1)}, response: []any{"v"}, status: 200},
		{method: "POST", expectedBody: []any{"BLPOP", "k", float64(1)}, response: []any{"k", "v"}, status: 200},
		{method: "POST", expectedBody: []any{"BRPOP", "k", float64(1)}, response: []any{"k", "v"}, status: 200},
		{method: "POST", expectedBody: []any{"ZINCRBY", "zs", 1.1, "m"}, response: "2.2", status: 200},
		{method: "POST", expectedBody: []any{"ZMSCORE", "zs", "m1"}, response: []any{"1.1"}, status: 200},
		{method: "POST", expectedBody: []any{"ZPOPMAX", "zs", float64(1)}, response: []any{"m1"}, status: 200},
		{method: "POST", expectedBody: []any{"ZPOPMIN", "zs", float64(1)}, response: []any{"m1"}, status: 200},
		{method: "POST", expectedBody: []any{"BZPOPMAX", "k", float64(1)}, response: []any{"k", "m", "1.1"}, status: 200},
		{method: "POST", expectedBody: []any{"BZPOPMIN", "k", float64(1)}, response: []any{"k", "m", "1.1"}, status: 200},
		{method: "POST", expectedBody: []any{"ZUNION", float64(2), "k1", "k2"}, response: []any{"v"}, status: 200},
	})
	defer close()

	ctx := context.Background()
	_, _ = u.HSetNX(ctx, "h", "f", "v")
	_, _ = u.HStrLen(ctx, "h", "f")
	_, _ = u.HVals(ctx, "h")
	_, _ = u.LInsert(ctx, "l", "BEFORE", "p", "e")
	_, _ = u.LMove(ctx, "s", "d", "LEFT", "RIGHT")
	_, _ = u.LPos(ctx, "l", "e")
	_, _ = u.LSet(ctx, "l", 0, "v")
	_, _ = u.RPopLPush(ctx, "s", "d")
	_, _ = u.SDiffStore(ctx, "d", "k1", "k2")
	_, _ = u.SInterStore(ctx, "d", "k1", "k2")
	_, _ = u.SMove(ctx, "s", "d", "m")
	_, _ = u.SUnion(ctx, "k1", "k2")
	_, _ = u.SUnionStore(ctx, "d", "k1", "k2")
	_, _ = u.ZDiff(ctx, 2, []string{"k1", "k2"})
	_, _ = u.ZLexCount(ctx, "zs", "a", "b")
	_, _ = u.ZMPop(ctx, 1, []string{"k"}, "MAX", 1)
	_, _ = u.ZRemRangeByLex(ctx, "zs", "a", "b")
	_, _ = u.ZRemRangeByScore(ctx, "zs", 0, 10)
	_, _ = u.ZRevRange(ctx, "zs", 0, -1)
	_, _ = u.ZRevRank(ctx, "zs", "m")
	_, _ = u.ZUnionStore(ctx, "d", 2, []string{"k1", "k2"})
	_, _ = u.ZRevRangeByLex(ctx, "zs", "max", "min", 1)
	_, _ = u.ZRevRangeByScore(ctx, "zs", "max", "min", 1)
	_, _ = u.BLPop(ctx, 1, "k")
	_, _ = u.BRPop(ctx, 1, "k")
	_, _ = u.ZIncrBy(ctx, "zs", 1.1, "m")
	_, _ = u.ZMScore(ctx, "zs", "m1")
	_, _ = u.ZPopMax(ctx, "zs", 1)
	_, _ = u.ZPopMin(ctx, "zs", 1)
	_, _ = u.BZPopMax(ctx, 1, "k")
	_, _ = u.BZPopMin(ctx, 1, "k")
	_, _ = u.ZUnion(ctx, 2, []string{"k1", "k2"})
}

func TestUnitJsonStreamCompletionist(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{method: "POST", expectedBody: []any{"JSON.MGET", "k1", "k2", "$"}, response: []any{map[string]any{"a": float64(1)}}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.TYPE", "doc", "$"}, response: "object", status: 200},
		{method: "POST", expectedBody: []any{"JSON.ARRAPPEND", "doc", "$", float64(1)}, response: []any{float64(1)}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.ARRLEN", "doc", "$"}, response: []any{float64(1)}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.FORGET", "doc", "$"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"JSON.MERGE", "doc", "$", map[string]any{"b": float64(2)}}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"JSON.NUMINCRBY", "doc", "$", float64(1)}, response: "2", status: 200},
		{method: "POST", expectedBody: []any{"JSON.OBJLEN", "doc", "$"}, response: []any{float64(1)}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.STRAPPEND", "doc", "$", "v"}, response: []any{float64(2)}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.STRLEN", "doc", "$"}, response: []any{float64(2)}, status: 200},
		{method: "POST", expectedBody: []any{"XREVRANGE", "s", "+", "-", "COUNT", float64(10)}, response: []any{[]any{"id", []any{"f", "v"}}}, status: 200},
		{method: "POST", expectedBody: []any{"XDEL", "s", "id1"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"XGROUP", "CREATE", "s", "g", "$"}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"XREAD", "COUNT", float64(1), "BLOCK", float64(0), "STREAMS", "s1", "$"}, response: []any{}, status: 200},
		{method: "POST", expectedBody: []any{"FCALL_RO", "f", float64(1), "k", "a"}, response: "res", status: 200},
		{method: "POST", expectedBody: []any{"FUNCTION", "DELETE", "L"}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"FUNCTION", "FLUSH"}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"FUNCTION", "STATS"}, response: map[string]any{}, status: 200},
	})
	defer close()

	ctx := context.Background()
	_, _ = u.JsonMGet(ctx, "$", "k1", "k2")
	_, _ = u.JsonType(ctx, "doc", "$")
	_, _ = u.JsonArrAppend(ctx, "doc", "$", 1)
	_, _ = u.JsonArrLen(ctx, "doc", "$")
	_, _ = u.JsonForget(ctx, "doc", "$")
	_, _ = u.JsonMerge(ctx, "doc", "$", map[string]any{"b": 2})
	_, _ = u.JsonNumIncrBy(ctx, "doc", "$", 1)
	_, _ = u.JsonObjLen(ctx, "doc", "$")
	_, _ = u.JsonStrAppend(ctx, "doc", "$", "v")
	_, _ = u.JsonStrLen(ctx, "doc", "$")
	_, _ = u.XRevRange(ctx, "s", "+", "-", 10)
	_, _ = u.XDel(ctx, "s", "id1")
	_, _ = u.XGroup(ctx, "CREATE", "s", "g", "$")
	_, _ = u.XRead(ctx, 1, 0, map[string]string{"s1": "$"})
	_, _ = u.FCallRO(ctx, "f", []string{"k"}, "a")
	_, _ = u.FunctionDelete(ctx, "L")
	_, _ = u.FunctionFlush(ctx)
	_, _ = u.FunctionStats(ctx)
}

func TestUnitParityFinalBoss(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{method: "POST", expectedBody: []any{"JSON.ARRINDEX", "k", "$", float64(1)}, response: []any{float64(0)}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.ARRINSERT", "k", "$", float64(0), float64(1)}, response: []any{float64(1)}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.ARRPOP", "k", "$"}, response: []any{float64(1)}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.ARRTRIM", "k", "$", float64(0), float64(1)}, response: []any{float64(1)}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.NUMMULTBY", "k", "$", float64(2)}, response: "2", status: 200},
		{method: "POST", expectedBody: []any{"XAUTOCLAIM", "k", "g", "c", float64(100), "0"}, response: []any{}, status: 200},
		{method: "POST", expectedBody: []any{"XCLAIM", "k", "g", "c", float64(100), "id1"}, response: []any{}, status: 200},
		{method: "POST", expectedBody: []any{"XINFO", "STREAM", "k"}, response: []any{}, status: 200},
		{method: "POST", expectedBody: []any{"XPENDING", "k", "g"}, response: []any{}, status: 200},
		{method: "POST", expectedBody: []any{"XREADGROUP", "GROUP", "g", "c", "BLOCK", float64(0), "STREAMS", "s1", ">"}, response: []any{}, status: 200},
		{method: "POST", expectedBody: []any{"WAIT", float64(1), float64(100)}, response: float64(1), status: 200},
	})
	defer close()

	ctx := context.Background()
	_, _ = u.JsonArrIndex(ctx, "k", "$", 1)
	_, _ = u.JsonArrInsert(ctx, "k", "$", 0, 1)
	_, _ = u.JsonArrPop(ctx, "k", "$")
	_, _ = u.JsonArrTrim(ctx, "k", "$", 0, 1)
	_, _ = u.JsonNumMultBy(ctx, "k", "$", 2)
	_, _ = u.XAutoClaim(ctx, "k", "g", "c", 100, "0")
	_, _ = u.XClaim(ctx, "k", "g", "c", 100, "id1")
	_, _ = u.XInfo(ctx, "STREAM", "k")
	_, _ = u.XPending(ctx, "k", "g")
	_, _ = u.XReadGroup(ctx, upstash.XReadGroupOptions{Group: "g", Consumer: "c", Block: 0}, map[string]string{"s1": ">"})
	_, _ = u.Wait(ctx, 1, 100)
}

func TestUnitCoverageElitePush(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{method: "POST", expectedBody: []any{"PEXPIRE", "k", float64(1000)}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"PTTL", "k"}, response: float64(500), status: 200},
		{method: "POST", expectedBody: []any{"MIGRATE", "h", "p", "k", "db", float64(100), "COPY", "REPLACE", "KEYS", "k1"}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"OBJECT", "encoding", "k"}, response: "raw", status: 200},
		{method: "POST", expectedBody: []any{"SORT", "k", "ALPHA"}, response: []any{"a"}, status: 200},
		{method: "POST", expectedBody: []any{"HINCRBY", "h", "f", float64(1)}, response: float64(2), status: 200},
		{method: "POST", expectedBody: []any{"HMGET", "h", "f1"}, response: []any{"v1"}, status: 200},
		{method: "POST", expectedBody: []any{"JSON.DEL", "doc", "$"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"LRANGE", "l", float64(0), float64(-1)}, response: []any{"v1"}, status: 200},
		{method: "POST", expectedBody: []any{"RPUSHX", "l", "v1"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"ZRANK", "zs", "m"}, response: float64(0), status: 200},
		{method: "POST", expectedBody: []any{"XRANGE", "s", "-", "+", "COUNT", float64(1)}, response: []any{}, status: 200},
		{method: "POST", expectedBody: []any{"GETDEL", "k"}, response: "v", status: 200},
		{method: "POST", expectedBody: []any{"INFO", "cpu"}, response: "info", status: 200},
		{method: "POST", expectedBody: []any{"ROLE"}, response: []any{"master"}, status: 200},
		{method: "POST", expectedBody: []any{"RENAMENX", "k1", "k2"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"GEOPOS", "sicily", "Palermo"}, response: []any{[]any{"13.36", "38.11"}}, status: 200},
		{method: "POST", expectedBody: []any{"SPOP", "s", float64(1)}, response: []any{"m1"}, status: 200},
		{method: "POST", expectedBody: []any{"SRANDMEMBER", "s", float64(1)}, response: []any{"m1"}, status: 200},
		{method: "POST", expectedBody: []any{"EVALSHA", "sha", float64(1), "k", "a"}, response: "res", status: 200},
		{method: "GET", path: "/monitor", response: "OK", status: 200},
	})
	defer close()

	ctx := context.Background()
	_, _ = u.PExpire(ctx, "k", 1000)
	_, _ = u.PTtl(ctx, "k")
	_, _ = u.Migrate(ctx, "h", "p", "k", "db", 100, true, true, "k1")
	_, _ = u.Object(ctx, "encoding", "k")
	_, _ = u.Sort(ctx, "k", "ALPHA")
	_, _ = u.HIncrBy(ctx, "h", "f", 1)
	_, _ = u.HMGet(ctx, "h", "f1")
	_, _ = u.JsonDel(ctx, "doc", "$")
	_, _ = u.LRange(ctx, "l", 0, -1)
	_, _ = u.RPushX(ctx, "l", "v1")
	_, _ = u.ZRank(ctx, "zs", "m")
	_, _ = u.XRange(ctx, "s", "-", "+", 1)
	_, _ = u.GetDel(ctx, "k")
	_, _ = u.Info(ctx, "cpu")
	_, _ = u.Role(ctx)
	_, _ = u.RenameNX(ctx, "k1", "k2")
	_, _ = u.GeoPos(ctx, "sicily", "Palermo")
	_, _ = u.SPop(ctx, "s", 1)
	_, _ = u.SRandMember(ctx, "s", 1)
	_, _ = u.EvalSha(ctx, "sha", []string{"k"}, "a")
	_, _ = u.Monitor(ctx)
}

func TestUnitAbsoluteFinalParity(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{method: "POST", expectedBody: []any{"GEOSEARCH", "k", "FROMMEMBER", "m", "BYRADIUS", float64(100), "km"}, response: []any{"m1"}, status: 200},
		{method: "POST", expectedBody: []any{"GEOSEARCHSTORE", "d", "s", "FROMMEMBER", "m", "BYRADIUS", float64(100), "km"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"PUBSUB", "CHANNELS"}, response: []any{"ch1"}, status: 200},
		{method: "POST", expectedBody: []any{"UNSUBSCRIBE", "ch1"}, response: []any{"unsubscribed", "ch1", float64(0)}, status: 200},
		{method: "POST", expectedBody: []any{"WATCH", "k1"}, response: "OK", status: 200},
		{method: "POST", expectedBody: []any{"UNWATCH"}, response: "OK", status: 200},
	})
	defer close()

	ctx := context.Background()
	_, _ = u.GeoSearch(ctx, "k", "FROMMEMBER", "m", "BYRADIUS", 100, "km")
	_, _ = u.GeoSearchStore(ctx, "d", "s", "FROMMEMBER", "m", "BYRADIUS", 100, "km")
	_, _ = u.PubSub(ctx, "CHANNELS")
	_, _ = u.Unsubscribe(ctx, "ch1")
	_, _ = u.Watch(ctx, "k1")
	_, _ = u.Unwatch(ctx)

	tx := u.Tx()
	tx.Push("SET", "k", "v")
	tx.Discard()
	txRes, _ := tx.Exec(ctx)
	require.Len(t, txRes, 0)
}

func TestUnitBitmapsCompletionist(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{method: "POST", expectedBody: []any{"BITOP", "AND", "dest", "k1", "k2"}, response: float64(1), status: 200},
		{method: "POST", expectedBody: []any{"BITPOS", "k1", float64(1), float64(0), float64(10)}, response: float64(5), status: 200},
		{method: "POST", expectedBody: []any{"BITFIELD", "k1", "GET", "u8", float64(0)}, response: []any{float64(1)}, status: 200},
		{method: "POST", expectedBody: []any{"BITFIELD_RO", "k1", "GET", "u8", float64(0)}, response: []any{float64(1)}, status: 200},
	})
	defer close()

	ctx := context.Background()
	_, _ = u.BitOp(ctx, "AND", "dest", "k1", "k2")
	_, _ = u.BitPos(ctx, "k1", 1, 0, 10)
	_, _ = u.BitField(ctx, "k1", "GET", "u8", 0)
	_, _ = u.BitFieldRO(ctx, "k1", "GET", "u8", 0)
}

func TestUnitGeoMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"GEOADD", "sicily", 13.361389, 38.115556, "Palermo"},
			response:     float64(1),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"GEODIST", "sicily", "Palermo", "Catania", "km"},
			response:     "166.27",
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()
	res, err := u.GeoAdd(ctx, "sicily", upstash.GeoLocation{Longitude: 13.361389, Latitude: 38.115556, Member: "Palermo"})
	require.NoError(t, err)
	require.Equal(t, 1, res)

	dist, err := u.GeoDist(ctx, "sicily", "Palermo", "Catania", "km")
	require.NoError(t, err)
	require.Equal(t, 166.27, dist)
}

func TestUnitJsonMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"JSON.SET", "doc", "$", map[string]any{"a": float64(1)}},
			response:     "OK",
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"JSON.GET", "doc", "$"},
			response:     []any{map[string]any{"a": float64(1)}},
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()
	res, err := u.JsonSet(ctx, "doc", "$", map[string]any{"a": 1})
	require.NoError(t, err)
	require.Equal(t, "OK", res)

	val, err := u.JsonGet(ctx, "doc", "$")
	require.NoError(t, err)
	require.NotNil(t, val)
}

func TestUnitStreamMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"XADD", "mystream", "*", "f1", "v1"},
			response:     "1518390000000-0",
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"XLEN", "mystream"},
			response:     float64(1),
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()
	id, err := u.XAdd(ctx, "mystream", "*", map[string]string{"f1": "v1"})
	require.NoError(t, err)
	require.Equal(t, "1518390000000-0", id)

	len, err := u.XLen(ctx, "mystream")
	require.NoError(t, err)
	require.Equal(t, 1, len)
}

func TestUnitScriptingMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"EVAL", "return ARGV[1]", float64(0), "hello"},
			response:     "hello",
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"SCRIPT", "LOAD", "return 1"},
			response:     "sha1hash",
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()
	res, err := u.Eval(ctx, "return ARGV[1]", []string{}, "hello")
	require.NoError(t, err)
	require.Equal(t, "hello", res)

	sha, err := u.ScriptLoad(ctx, "return 1")
	require.NoError(t, err)
	require.Equal(t, "sha1hash", sha)
}

func TestUnitConnectionMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"PING"},
			response:     "PONG",
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"ECHO", "hello"},
			response:     "hello",
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()
	res, err := u.Ping(ctx)
	require.NoError(t, err)
	require.Equal(t, "PONG", res)

	res, err = u.Echo(ctx, "hello")
	require.NoError(t, err)
	require.Equal(t, "hello", res)
}

func TestUnitGenericMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"DEL", "k1", "k2"},
			response:     float64(2),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"EXISTS", "k1"},
			response:     float64(1),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"EXPIRE", "k1", float64(10)},
			response:     float64(1),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"TTL", "k1"},
			response:     float64(5),
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()

	res, err := u.Del(ctx, "k1", "k2")
	require.NoError(t, err)
	require.Equal(t, 2, res)

	res, err = u.Exists(ctx, "k1")
	require.NoError(t, err)
	require.Equal(t, 1, res)

	res, err = u.Expire(ctx, "k1", 10)
	require.NoError(t, err)
	require.Equal(t, 1, res)

	res, err = u.Ttl(ctx, "k1")
	require.NoError(t, err)
	require.Equal(t, 5, res)
}

func TestUnitSetMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"SADD", "s", "m1", "m2"},
			response:     float64(2),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"SMEMBERS", "s"},
			response:     []any{"m1", "m2"},
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"SREM", "s", "m1"},
			response:     float64(1),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"SISMEMBER", "s", "m2"},
			response:     float64(1),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"SCARD", "s"},
			response:     float64(1),
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()

	res, err := u.SAdd(ctx, "s", "m1", "m2")
	require.NoError(t, err)
	require.Equal(t, 2, res)

	members, err := u.SMembers(ctx, "s")
	require.NoError(t, err)
	require.Equal(t, []string{"m1", "m2"}, members)

	res, err = u.SRem(ctx, "s", "m1")
	require.NoError(t, err)
	require.Equal(t, 1, res)

	isMem, err := u.SIsMember(ctx, "s", "m2")
	require.NoError(t, err)
	require.Equal(t, 1, isMem)

	card, err := u.SCard(ctx, "s")
	require.NoError(t, err)
	require.Equal(t, 1, card)
}

func TestUnitSortedSetMethods(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"ZADD", "zs", float64(1), "m1"},
			response:     float64(1),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"ZRANGE", "zs", float64(0), float64(-1)},
			response:     []any{"m1"},
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"ZSCORE", "zs", "m1"},
			response:     "1.0",
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"ZREM", "zs", "m1"},
			response:     float64(1),
			status:       200,
		},
		{
			method:       "POST",
			expectedBody: []any{"ZCARD", "zs"},
			response:     float64(0),
			status:       200,
		},
	})
	defer close()

	ctx := context.Background()

	res, err := u.ZAdd(ctx, "zs", 1, "m1")
	require.NoError(t, err)
	require.Equal(t, 1, res)

	rangeRes, err := u.ZRange(ctx, "zs", 0, -1)
	require.NoError(t, err)
	require.Equal(t, []string{"m1"}, rangeRes)

	score, err := u.ZScore(ctx, "zs", "m1")
	require.NoError(t, err)
	require.Equal(t, 1.0, score)

	removed, err := u.ZRem(ctx, "zs", "m1")
	require.NoError(t, err)
	require.Equal(t, 1, removed)

	card, err := u.ZCard(ctx, "zs")
	require.NoError(t, err)
	require.Equal(t, 0, card)
}

func TestUnitPublish(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:       "POST",
			expectedBody: []any{"PUBLISH", "ch", "msg"},
			response:     float64(1),
			status:       200,
		},
	})
	defer close()

	res, err := u.Publish(context.Background(), "ch", "msg")
	require.NoError(t, err)
	require.Equal(t, 1, res)
}

func TestUnitSubscribe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)

		_, _ = fmt.Fprint(w, "data: \"hello\"\n\n")
		flusher.Flush()
		_, _ = fmt.Fprint(w, "data: world\n\n")
		flusher.Flush()
	}))
	defer server.Close()

	u, _ := upstash.New(upstash.Options{Url: server.URL, Token: "t"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs, err := u.Subscribe(ctx, "ch")
	require.NoError(t, err)

	require.Equal(t, "hello", <-msgs)
	require.Equal(t, "world", <-msgs)
}
