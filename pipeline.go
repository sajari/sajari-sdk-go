package sajari

import (
	"golang.org/x/net/context"

	piplinepb "code.sajari.com/protogen-go/sajari/api/pipeline/v1"
)

// Pipeline returns a Pipeline for querying a collection.
func (c *Client) Pipeline(name string) *Pipeline {
	return &Pipeline{
		name: name,
		c:    c,
	}
}

// Pipeline is a handler for a named pipeline.
type Pipeline struct {
	name string

	c *Client
}

// Search runs a search query defined by a pipline with the given values and
// tracking configuration.  Returns the query results and returned values (which could have
// been modified in the pipeline).
func (p *Pipeline) Search(ctx context.Context, values map[string]string, tracking Tracking) (*Results, map[string]string, error) {
	pbTracking, err := tracking.proto()
	if err != nil {
		return nil, nil, err
	}

	r := &piplinepb.SearchRequest{
		Pipeline: &piplinepb.Pipeline{
			Name: p.name,
		},
		Tracking: pbTracking,
		Values:   values,
	}

	resp, err := piplinepb.NewQueryClient(p.c.ClientConn).Search(p.c.newContext(ctx), r)
	if err != nil {
		return nil, nil, err
	}

	results, err := processResponse(resp.SearchResponse, resp.Tokens)
	if err != nil {
		return nil, nil, err
	}
	return results, resp.Values, nil
}
