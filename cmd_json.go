package upstash

import (
	"context"
	"fmt"
)

// JsonSet sets the JSON value at path in key.
func (u *Upstash) JsonSet(ctx context.Context, key, path string, value any) (string, error) {
	res, err := u.Send(ctx, "JSON.SET", key, path, value)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// JsonGet returns the value at path in key.
func (u *Upstash) JsonGet(ctx context.Context, key string, paths ...string) (any, error) {
	args := make([]any, 0, 1+len(paths))
	args = append(args, key)
	for _, p := range paths {
		args = append(args, p)
	}
	return u.Send(ctx, "JSON.GET", args...)
}

// JsonDel deletes the value at path in key.
func (u *Upstash) JsonDel(ctx context.Context, key, path string) (int, error) {
	res, err := u.Send(ctx, "JSON.DEL", key, path)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// JsonMGet returns the values at path in multiple keys.
func (u *Upstash) JsonMGet(ctx context.Context, path string, keys ...string) ([]any, error) {
	args := make([]any, 0, len(keys)+1)
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, path)
	res, err := u.Send(ctx, "JSON.MGET", args...)
	if err != nil {
		return nil, err
	}
	return res.([]any), nil
}

// JsonType returns the type of the JSON value at path in key.
func (u *Upstash) JsonType(ctx context.Context, key, path string) (string, error) {
	res, err := u.Send(ctx, "JSON.TYPE", key, path)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// JsonArrAppend appends the JSON values to the array at path in key.
func (u *Upstash) JsonArrAppend(ctx context.Context, key, path string, values ...any) ([]int, error) {
	args := make([]any, 0, 2+len(values))
	args = append(args, key, path)
	args = append(args, values...)
	res, err := u.Send(ctx, "JSON.ARRAPPEND", args...)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([]int, len(list))
	for i, v := range list {
		if v != nil {
			result[i] = int(v.(float64))
		}
	}
	return result, nil
}

// JsonArrLen returns the length of the array at path in key.
func (u *Upstash) JsonArrLen(ctx context.Context, key, path string) ([]int, error) {
	res, err := u.Send(ctx, "JSON.ARRLEN", key, path)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([]int, len(list))
	for i, v := range list {
		if v != nil {
			result[i] = int(v.(float64))
		}
	}
	return result, nil
}

// JsonClear removes container values (list, set, hash) or zeros numeric values.
func (u *Upstash) JsonClear(ctx context.Context, key string, path ...string) (int, error) {
	args := make([]any, 0, 1+len(path))
	args = append(args, key)
	for _, p := range path {
		args = append(args, p)
	}
	res, err := u.Send(ctx, "JSON.CLEAR", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// JsonForget is an alias for JsonDel.
func (u *Upstash) JsonForget(ctx context.Context, key string, path ...string) (int, error) {
	args := make([]any, 0, 1+len(path))
	args = append(args, key)
	for _, p := range path {
		args = append(args, p)
	}
	res, err := u.Send(ctx, "JSON.FORGET", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// JsonMerge merges a JSON value into a key at a given path.
func (u *Upstash) JsonMerge(ctx context.Context, key, path string, value any) (string, error) {
	res, err := u.Send(ctx, "JSON.MERGE", key, path, value)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

// JsonNumIncrBy increments a number in a JSON document by a given value.
func (u *Upstash) JsonNumIncrBy(ctx context.Context, key, path string, value float64) (string, error) {
	res, err := u.Send(ctx, "JSON.NUMINCRBY", key, path, value)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(res), nil
}

// JsonObjKeys returns the keys in the object at path in key.
func (u *Upstash) JsonObjKeys(ctx context.Context, key, path string) ([]string, error) {
	res, err := u.Send(ctx, "JSON.OBJKEYS", key, path)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	list := res.([]any)
	result := make([]string, len(list))
	for i, v := range list {
		result[i] = v.(string)
	}
	return result, nil
}

// JsonObjLen returns the number of keys in the object at path in key.
func (u *Upstash) JsonObjLen(ctx context.Context, key, path string) ([]int, error) {
	res, err := u.Send(ctx, "JSON.OBJLEN", key, path)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([]int, len(list))
	for i, v := range list {
		if v != nil {
			result[i] = int(v.(float64))
		}
	}
	return result, nil
}

// JsonStrAppend appends a string to the JSON string value at path in key.
func (u *Upstash) JsonStrAppend(ctx context.Context, key, path, value string) ([]int, error) {
	res, err := u.Send(ctx, "JSON.STRAPPEND", key, path, value)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([]int, len(list))
	for i, v := range list {
		if v != nil {
			result[i] = int(v.(float64))
		}
	}
	return result, nil
}

// JsonStrLen returns the length of the JSON string value at path in key.
func (u *Upstash) JsonStrLen(ctx context.Context, key, path string) ([]int, error) {
	res, err := u.Send(ctx, "JSON.STRLEN", key, path)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([]int, len(list))
	for i, v := range list {
		if v != nil {
			result[i] = int(v.(float64))
		}
	}
	return result, nil
}

// JsonToggle toggles a boolean value at path in key.
func (u *Upstash) JsonToggle(ctx context.Context, key, path string) (any, error) {
	return u.Send(ctx, "JSON.TOGGLE", key, path)
}

// JsonArrIndex returns the index of the first occurrence of a JSON value in an array.
func (u *Upstash) JsonArrIndex(ctx context.Context, key, path string, value any, startEnd ...int) ([]int, error) {
	args := []any{key, path, value}
	for _, se := range startEnd {
		args = append(args, se)
	}
	res, err := u.Send(ctx, "JSON.ARRINDEX", args...)
	if err != nil {
		return nil, err
	}
	return u.parseIntSlice(res), nil
}

// JsonArrInsert inserts JSON values into an array at a given index.
func (u *Upstash) JsonArrInsert(ctx context.Context, key, path string, index int, values ...any) ([]int, error) {
	args := make([]any, 0, 3+len(values))
	args = append(args, key, path, index)
	args = append(args, values...)
	res, err := u.Send(ctx, "JSON.ARRINSERT", args...)
	if err != nil {
		return nil, err
	}
	return u.parseIntSlice(res), nil
}

// JsonArrPop removes and returns an element from an array.
func (u *Upstash) JsonArrPop(ctx context.Context, key, path string, index ...int) ([]any, error) {
	args := []any{key, path}
	if len(index) > 0 {
		args = append(args, index[0])
	}
	res, err := u.Send(ctx, "JSON.ARRPOP", args...)
	if err != nil {
		return nil, err
	}
	return res.([]any), nil
}

// JsonArrTrim trims an array to contain only the specified range of elements.
func (u *Upstash) JsonArrTrim(ctx context.Context, key, path string, start, stop int) ([]int, error) {
	res, err := u.Send(ctx, "JSON.ARRTRIM", key, path, start, stop)
	if err != nil {
		return nil, err
	}
	return u.parseIntSlice(res), nil
}

// JsonNumMultBy multiplies a number in a JSON document by a given value.
func (u *Upstash) JsonNumMultBy(ctx context.Context, key, path string, value float64) (string, error) {
	res, err := u.Send(ctx, "JSON.NUMMULTBY", key, path, value)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(res), nil
}

func (u *Upstash) parseIntSlice(res any) []int {
	list := res.([]any)
	result := make([]int, len(list))
	for i, v := range list {
		if v != nil {
			result[i] = int(v.(float64))
		}
	}
	return result
}
