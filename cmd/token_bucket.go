package cmd

import (
	"log"
	"time"
)

type TokenBucketAPI interface {
	Get()
	Put()
}

type TokenBucket struct {
	ch chan uint8
}

func CreateTokenBucket(size int) *TokenBucket {
	tokenBucket := make(chan uint8, size)
	for i := 0; i < cap(tokenBucket) && cap(tokenBucket) > len(tokenBucket); i++ {
		tokenBucket <- 0
	}
	go func(tokenBucket chan uint8) {
		defer func() {
			log.Fatal("token bucket put thread interrupt")
		}()
		for {
			if cap(tokenBucket) > len(tokenBucket) {
				tokenBucket <- 0
			}
			time.Sleep(time.Second / time.Duration(cap(tokenBucket)))
		}
	}(tokenBucket)
	return &TokenBucket{ch: tokenBucket}
}

func (t *TokenBucket) Get() {
	<-t.ch
}

func (t *TokenBucket) Put() {
	t.ch <- 0
}
