package sajari

import "google.golang.org/grpc"

// Opt is a type which defines Client options.
type Opt func(c *Client)

// WithEndpoint configures the client to use a custom endpoint.
func WithEndpoint(endpoint string) Opt {
	return func(c *Client) {
		c.endpoint = endpoint
	}
}

// WithCredentials sets the client credentials used in each request.
func WithCredentials(c Credentials) Opt {
	return WithGRPCDialOption(grpc.WithPerRPCCredentials(creds{c}))
}

// WithGRPCDialOption returns an Opt which appends a new grpc.DialOption
// to an underlying gRPC dial.
func WithGRPCDialOption(opt grpc.DialOption) Opt {
	return func(c *Client) {
		c.dialOpts = append(c.dialOpts, opt)
	}
}
