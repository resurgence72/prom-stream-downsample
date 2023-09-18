package agg

import (
	"errors"

	"prom-stream-downsample/pkg/pb"
)

type AggFn func([]pb.Point) any

type Agg struct {
	name string

	fn AggFn
}

func NewAgg(name string) (Agg, error) {
	fn, ok := aggFnMap[name]
	if !ok {
		return Agg{}, errors.New("agg name not found")
	}
	return Agg{name: name, fn: fn}, nil
}

func (a Agg) Aggregate(points []pb.Point) any {
	return a.fn(points)
}

func (a Agg) Name() string {
	return a.name
}
