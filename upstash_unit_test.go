package upstash_test

import (
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
			err := json.NewDecoder(r.Body).Decode(&body)
			require.NoError(t, err)
			require.Equal(t, h.expectedBody, body)
		}

		// Send Response
		w.WriteHeader(h.status)
		json.NewEncoder(w).Encode(map[string]any{
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

func TestUnitGet(t *testing.T) {
	u, close := setupMockServer(t, []mockHandler{
		{
			method:   "GET",
			response: "bar",
			status:   200,
		},
	})
	defer close()

	val, err := u.Get("foo")
	require.NoError(t, err)
	require.Equal(t, "bar", val)
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

	err := u.Set("foo", "bar")
	require.NoError(t, err)
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

	val, err := u.Incr("counter")
	require.NoError(t, err)
	require.Equal(t, 5, val)
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

	vals, err := u.MGet([]string{"k1", "k2"})
	require.NoError(t, err)
	require.Equal(t, []string{"v1", "v2"}, vals)
}

func TestUnitError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{
			"error": "ERR wrong number of arguments",
		})
	}))
	defer server.Close()

	u, err := upstash.New(upstash.Options{
		Url:   server.URL,
		Token: "mock-token",
	})
	require.NoError(t, err)

	_, err = u.Get("foo") 
	require.Error(t, err)
	require.Contains(t, err.Error(), "ERR wrong number of arguments")
}
