// Package sajari provides functionality for interacting with Sajari APIs.
package sajari // import "code.sajari.com/sajari-sdk-go"

import (
	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.sajari.com/sajari-sdk-go/internal"
)

const (
	endpoint  = "api.sajari.com:443"
	userAgent = "sdk-go-07072016"
)

// New creates a new Client which can be used to make requests to Sajari services.
func New(project, collection string, opts ...Opt) (*Client, error) {
	c := &Client{
		Project:    project,
		Collection: collection,
	}

	defaultOpts := []Opt{
		WithEndpoint(endpoint),
		WithGRPCDialOption(grpc.WithUserAgent(userAgent)),
		WithGRPCDialOption(grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "api.sajari.com"))),
	}

	opts = append(defaultOpts, opts...)
	for _, opt := range opts {
		opt(c)
	}

	if c.ClientConn == nil {
		conn, err := grpc.Dial(c.endpoint, c.dialOpts...)
		if err != nil {
			return nil, err
		}
		c.ClientConn = conn
	}
	return c, nil
}

func (c *Client) newContext(ctx context.Context) context.Context {
	return internal.NewContext(ctx, c.Project, c.Collection)
}

// Client is a type which makes requests to the Sajari Engine.
type Client struct {
	Project    string
	Collection string
	endpoint   string

	ClientConn *grpc.ClientConn
	dialOpts   []grpc.DialOption
}

// Close releases all resources held by the Client.
func (c *Client) Close() error {
	return c.ClientConn.Close()
}
