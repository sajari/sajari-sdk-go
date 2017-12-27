package sajari

import (
	"fmt"

	"golang.org/x/net/context"

	recpb "code.sajari.com/protogen-go/sajari/engine/store/record"
)

// Learn takes a list of records identified by keys ks and a query request r and applies pos and neg
// weighting to the intersections of r and the record.
func (c *Client) LearnMulti(ctx context.Context, ks []*Key, r Request, counts []int, scores []float32) error {
	if len(ks) != len(counts) || len(ks) != len(scores) {
		return fmt.Errorf("number of keys, counts and scores do not match")
	}

	ars, err := c.Query().AnalyseMulti(ctx, ks, r)
	if err != nil {
		return err
	}

	pbks, err := keys(ks).proto()
	if err != nil {
		return err
	}

	keysScores := make([]*recpb.KeyScores, 0, len(pbks))
	for i, k := range pbks {
		keysScores = append(keysScores, &recpb.KeyScores{
			Key: k,
			Scores: []*recpb.KeyScores_Score{
				&recpb.KeyScores_Score{
					Terms: ars[i],
					Count: int32(counts[i]),
					Score: scores[i],
				},
			},
		})
	}

	resp, err := recpb.NewScoreClient(c.ClientConn).Increment(c.newContext(ctx), &recpb.IncrementRequest{
		KeysScores: keysScores,
	})
	if err != nil {
		return err
	}
	return multiErrorFromRecordStatusProto(resp.Status)
}

// Learn takes a record identified by k and a query request r and applies pos and neg
// weighting to the intersections of r and the record.
func (c *Client) Learn(ctx context.Context, k *Key, r Request, count int, score float32) error {
	err := c.LearnMulti(ctx, []*Key{k}, r, []int{count}, []float32{score})
	if err != nil {
		if me, ok := err.(MultiError); ok {
			return me[0]
		}
	}
	return err
}
