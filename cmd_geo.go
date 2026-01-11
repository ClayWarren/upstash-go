package upstash

import (
	"context"
	"strconv"
)

// GeoAdd adds the specified geospatial items (longitude, latitude, name) to the specified key.
func (u *Upstash) GeoAdd(ctx context.Context, key string, locations ...GeoLocation) (int, error) {
	args := make([]any, 0, 1+len(locations)*3)
	args = append(args, key)
	for _, loc := range locations {
		args = append(args, loc.Longitude, loc.Latitude, loc.Member)
	}
	res, err := u.Send(ctx, "GEOADD", args...)
	if err != nil {
		return 0, err
	}
	return int(res.(float64)), nil
}

// GeoDist returns the distance between two members in the geospatial index.
func (u *Upstash) GeoDist(ctx context.Context, key, member1, member2, unit string) (float64, error) {
	res, err := u.Send(ctx, "GEODIST", key, member1, member2, unit)
	if err != nil {
		return 0, err
	}
	if res == nil {
		return 0, nil
	}
	return strconv.ParseFloat(res.(string), 64)
}

// GeoPos returns the longitude and latitude of all the specified members.
func (u *Upstash) GeoPos(ctx context.Context, key string, members ...string) ([][2]float64, error) {
	args := make([]any, 0, 1+len(members))
	args = append(args, key)
	for _, m := range members {
		args = append(args, m)
	}
	res, err := u.Send(ctx, "GEOPOS", args...)
	if err != nil {
		return nil, err
	}
	list := res.([]any)
	result := make([][2]float64, len(list))
	for i, v := range list {
		if v == nil {
			continue
		}
		pos := v.([]any)
		lng, _ := strconv.ParseFloat(pos[0].(string), 64)
		lat, _ := strconv.ParseFloat(pos[1].(string), 64)
		result[i] = [2]float64{lng, lat}
	}
	return result, nil
}
