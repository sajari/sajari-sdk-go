// Package autocomplete provides methods for interacting directly with auto complete models.
package autocomplete

import (
	"golang.org/x/net/context"

	"github.com/sajari/sajari-sdk-go"
	"github.com/sajari/sajari-sdk-go/internal"

	pb "github.com/sajari/protogen-go/sajari/autocomplete"
)

// New creates a new client for interacting with auto complete models.
func New(client *sajari.Client, name string) *Client {
	return &Client{
		c:    client,
		name: name,
	}
}

// Client provides methods for interacting directly with auto-complete models.
type Client struct {
	c *sajari.Client

	name string
}

func (c *Client) modelProto() *pb.Model {
	return &pb.Model{
		Name: c.name,
	}
}

func (c *Client) newContext(ctx context.Context) context.Context {
	return internal.NewContext(ctx, c.c.Project, c.c.Collection)
}

// TrainCorpus takes an array of terms and uses them to train an autocomplete model for spelling
// correction (i.e. these terms must correctly spelt).
func (c *Client) TrainCorpus(ctx context.Context, terms []string) error {
	_, err := pb.NewTrainClient(c.c.ClientConn).TrainCorpus(c.newContext(ctx), &pb.TrainCorpusRequest{
		Model: c.modelProto(),
		Terms: terms,
	})
	return err
}

// TrainQuery takes a query phrase and uses it to train an autocomplete model for partial queries. The
// phrase should be a successful query (i.e. good spelling and return useful results).
func (c *Client) TrainQuery(ctx context.Context, phrase string) error {
	_, err := pb.NewTrainClient(c.c.ClientConn).TrainQuery(c.newContext(ctx), &pb.TrainQueryRequest{
		Model:  c.modelProto(),
		Phrase: phrase,
	})
	return err
}

// Complete takes a phrase and its term components and returns an ordered array of
// potential completion matches. The terms are used to assist with spelling corrections
// and fuzzy matching, while the phrase is used as a prefix sequence.
func (c *Client) Complete(ctx context.Context, phrase string, terms []string) ([]string, error) {
	suggestions, err := pb.NewQueryClient(c.c.ClientConn).AutoComplete(c.newContext(ctx), &pb.AutoCompleteRequest{
		Model:  c.modelProto(),
		Phrase: phrase,
		Terms:  terms,
	})
	if err != nil {
		return nil, err
	}
	return suggestions.Phrases, nil
}
