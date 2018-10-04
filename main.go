package main

import (
	"context"
	"flag"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/MixinNetwork/ocean.one/cache"
	"github.com/MixinNetwork/ocean.one/config"
	"github.com/MixinNetwork/ocean.one/persistence"
	"github.com/go-redis/redis"
)

func main() {
	service := flag.String("service", "http", "run a service")
	flag.Parse()
  // new blank context 
	ctx := context.Background()
	// init spanner client
	spannerClient, err := spanner.NewClientWithConfig(ctx, config.GoogleCloudSpanner, spanner.ClientConfig{NumChannels: 4,
		SessionPoolConfig: spanner.SessionPoolConfig{
			HealthCheckInterval: 5 * time.Second,
		},
	})
	if err != nil {
		log.Panicln(err)
	}
  // init redis client due cache
	redisClient := redis.NewClient(&redis.Options{
		Addr:         config.RedisEngineCacheAddress,
		DB:           config.RedisEngineCacheDatabase,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  4 * time.Second,
		IdleTimeout:  60 * time.Second,
		PoolSize:     1024,
	})
	err = redisClient.Ping().Err() // ping redis server
	if err != nil {
		log.Panicln(err)
	}
  // inject spanner db into context
	ctx = persistence.SetupSpanner(ctx, spannerClient)
	// injext redis
	ctx = cache.SetupRedis(ctx, redisClient)

	switch *service {
	case "engine":
		NewExchange().Run(ctx) // 
	case "http":
		StartHTTP(ctx) // http server
	}
}
