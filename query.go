package sajari

import (
	"golang.org/x/net/context"

	pb "github.com/sajari/protogen-go/sajari/api/query/v1"
	querypb "github.com/sajari/protogen-go/sajari/engine/query/v1"
)

// Query returns a handler for running queries using the Client.
func (c *Client) Query() *Query {
	return &Query{c}
}

// Query is a handler which runs queries on a collection.
type Query struct {
	c *Client
}

// Search performs an engine search with the Request r, returning a set of Results and non-nil error
// if there was a problem.
func (q *Query) Search(ctx context.Context, r *Request) (*Results, error) {
	pr, err := r.proto()
	if err != nil {
		return nil, err
	}

	resp, err := pb.NewQueryClient(q.c.ClientConn).Search(q.c.newContext(ctx), pr)
	if err != nil {
		return nil, err
	}
	return processResponse(resp.SearchResponse, resp.Tokens)
}

// AnalyseMulti performs Analysis on multiple records against the same query request.
func (q *Query) AnalyseMulti(ctx context.Context, ks []*Key, r Request) ([][]string, error) {
	pr, err := r.proto()
	if err != nil {
		return nil, err
	}

	pbks, err := keys(ks).proto()
	if err != nil {
		return nil, err
	}

	resp, err := querypb.NewQueryClient(q.c.ClientConn).Analyse(q.c.newContext(ctx), &querypb.AnalyseRequest{
		SearchRequest: pr.SearchRequest,
		Keys:          pbks,
	})
	if err != nil {
		return nil, err
	}

	out := make([][]string, 0, len(resp.Terms))
	for _, ts := range resp.Terms {
		out = append(out, ts.Terms)
	}
	return out, multiErrorFromRecordStatusProto(resp.Status)
}

// Analyse returns the list of overlapping terms between the record identified by k and the search
// search request r.
func (q *Query) Analyse(ctx context.Context, k *Key, r Request) ([]string, error) {
	out, err := q.AnalyseMulti(ctx, []*Key{k}, r)
	if err != nil {
		if me, ok := err.(MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return out[0], nil
}
