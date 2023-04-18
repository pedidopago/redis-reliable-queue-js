// Package rq contains a queue that follows the reliable queue pattern.
// https://redis.io/commands/rpoplpush#pattern-reliable-queue
package rq

import (
	"context"
	"strconv"
	"strings"
	"time"
)

type Queue struct {
	RedisClient           *redis.Client
	Name                  string
	AckSuffix             string
	LeftPush              bool
	MessageExpiration     time.Duration // default 20m
	ListExpirationSeconds string        // default "3600" (1h)
}

func (q Queue) RestoreExpiredMessages(ctx context.Context) {
	lastIndex := 0
	for ctx.Err() == nil {
		items := q.RedisClient.LRange(ctx, q.getAckList(), int64(lastIndex), int64(lastIndex)+50).Val()
		if len(items) == 0 {
			break
		}
		lastIndex += len(items)
		for _, item := range items {
			itsplit := strings.SplitN(item, "|", 2)
			if len(itsplit) != 2 {
				q.RedisClient.LRem(ctx, q.getAckList(), 1, item)
			}
			timestamp, err := strconv.ParseInt(itsplit[0], 10, 64)
			if err != nil {
				q.RedisClient.LRem(ctx, q.getAckList(), 1, item)
				continue
			}
			if time.Now().Unix() > timestamp {
				// item expired, will be added back to the queue
				if q.LeftPush {
					q.RedisClient.LPush(ctx, q.Name, itsplit[1])
				} else {
					q.RedisClient.RPush(ctx, q.Name, itsplit[1])
				}
				q.RedisClient.LRem(ctx, q.getAckList(), 1, item)
			}
		}
	}
}
