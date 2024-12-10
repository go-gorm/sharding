package sharding

import "context"

// Define context key for sharding
type contextKey string

const (
	// ShardingQueryKey indicates the query is from sharding package
	ShardingQueryKey contextKey = "sharding_query"
)

// WithShardingContext adds sharding flag to context
func WithShardingContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, ShardingQueryKey, true)
}
