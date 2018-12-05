package nrredis

import (
	"strings"

	newrelic "github.com/newrelic/go-agent"
	"github.com/go-redis/redis"
)

// WrapRedisClient adds newrelic measurements for commands and returns cloned Client
func WrapRedisClient(txn newrelic.Transaction, c *redis.Client) *redis.Client {
	if txn == nil {
		return c
	}

	// clone using context
	ctx := c.Context()
	copy := c.WithContext(ctx)

	copy.WrapProcess(func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			defer segmentBuilder(txn, newrelic.DatastoreRedis, strings.Split(cmd.Name(), " ")[0]).End()

			return oldProcess(cmd)
		}
	})
	return copy
}

// WrapRedisClient adds newrelic measurements for commands and returns cloned ClusterClient
func WrapRedisClusterClient(txn newrelic.Transaction, c *redis.ClusterClient) *redis.ClusterClient {
	if txn == nil {
		return c
	}

	// clone using context
	ctx := c.Context()
	copy := c.WithContext(ctx)

	copy.WrapProcess(func(oldProcess func(cmd redis.Cmder) error) func(cmd redis.Cmder) error {
		return func(cmd redis.Cmder) error {
			defer segmentBuilder(txn, newrelic.DatastoreRedis, strings.Split(cmd.Name(), " ")[0]).End()

			return oldProcess(cmd)
		}
	})
	return copy
}

type segment interface {
	End() error
}

// create segment through function to be able to test it
var segmentBuilder = func(txn newrelic.Transaction, product newrelic.DatastoreProduct, operation string) segment {
	return &newrelic.DatastoreSegment{
		StartTime: newrelic.StartSegmentNow(txn),
		Product:   product,
		Operation: operation,
	}
}
