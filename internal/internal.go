package internal

import (
	"golang.org/x/net/context"

	"google.golang.org/grpc/metadata"
)

const (
	projectKey    = "project"
	collectionKey = "collection"
)

func NewContext(ctx context.Context, project, collection string) context.Context {
	m := map[string]string{
		projectKey:    project,
		collectionKey: collection,
	}
	return metadata.NewOutgoingContext(ctx, metadata.New(m))
}
