package util

import (
	"context"
	"math/rand"
	"time"
)

func jitter() {
	rand.Seed(time.Now().UnixNano())
	<-time.After(time.Duration(rand.Intn(5)) * time.Second)
}

func Wait(ctx context.Context, interval time.Duration, f func()) {
	for {
		jitter() // 随机等待一段时间，避免所有downsample同时请求prometheus
		f()
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}
