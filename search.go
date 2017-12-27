package sajari

import (
	"fmt"
	"strings"
	"time"

	pb "code.sajari.com/protogen-go/sajari/api/query/v1"
	querypb "code.sajari.com/protogen-go/sajari/engine/query/v1"
)

// TrackingType defines different modes of tracking which can be applied to search
// requests.
type TrackingType string

// TrackingType constants.
const (
	TrackingNone   TrackingType = ""        // No tracking is enabled.
	TrackingClick  TrackingType = "CLICK"   // Click tracking is enabled, Click tokens will be returned with results.
	TrackingPosNeg TrackingType = "POS_NEG" // Positive/negative interaction tokens should be returned with results.
)

func (t TrackingType) proto() (pb.SearchRequest_Tracking_Type, error) {
	switch t {
	case TrackingNone:
		return pb.SearchRequest_Tracking_NONE, nil

	case TrackingClick:
		return pb.SearchRequest_Tracking_CLICK, nil

	case TrackingPosNeg:
		return pb.SearchRequest_Tracking_POS_NEG, nil
	}
	return pb.SearchRequest_Tracking_NONE, fmt.Errorf("unknown TrackingType: %v", t)
}

// Tracking configures tracking for a search query.
type Tracking struct {
	// Tracking specifies which kind (if any) tokens should be generated and returned
	// with the query results.
	Type TrackingType

	// QueryID is a unique identifier for a single search query.  In the
	// case of live querying this is defined to be multiple individual queries
	// (i.e. as a user types the query is re-run).
	QueryID string

	// Sequence (i.e. sequential identifier) of this  in the context of a
	// sequence of queries.
	Sequence int

	// Field is the field to be used for adding identifier information to
	// generated tokens (see TrackingType).
	Field string

	// Data are values which will be recorded along with tracking data produced
	// for the request.
	Data map[string]string
}

func (t Tracking) proto() (*pb.SearchRequest_Tracking, error) {
	pbType, err := t.Type.proto()
	if err != nil {
		return nil, err
	}

	return &pb.SearchRequest_Tracking{
		Type:     pbType,
		QueryId:  t.QueryID,
		Sequence: int32(t.Sequence),
		Field:    t.Field,
		Data:     t.Data,
	}, nil
}

// IndexQuery is a query run against
type IndexQuery struct {
	// Text is the default body (free-text) of the query.
	// If set this has weight 1.0.
	Text string

	// Body is a list of weighted free-text.
	Body []Body

	// Terms is a list of pre-split terms.
	Terms []Term

	// FieldBoosts to be applied to the index score.
	FieldBoosts []FieldBoost

	// InstanceBoosts to be applied (change scoring of records).
	InstanceBoosts []InstanceBoost
}

func (q IndexQuery) proto() (*querypb.SearchRequest_IndexQuery, error) {
	iq := &querypb.SearchRequest_IndexQuery{}
	wb := q.Body
	if q.Text != "" {
		wb = append(wb, Body{
			Weight: 1.0,
			Text:   q.Text,
		})
	}
	for _, wb := range wb {
		iq.Body = append(iq.Body, wb.proto())
	}

	if len(q.Terms) > 0 {
		iq.Terms = terms(q.Terms).proto()
	}

	if q.FieldBoosts != nil {
		metaBoosts, err := fieldBoosts(q.FieldBoosts).proto()
		if err != nil {
			return nil, err
		}
		iq.FieldBoosts = metaBoosts
	}

	if q.InstanceBoosts != nil {
		indexBoosts, err := instanceBoosts(q.InstanceBoosts).proto()
		if err != nil {
			return nil, err
		}
		iq.InstanceBoosts = indexBoosts
	}
	return iq, nil
}

// FeatureQuery is a feature-based query which contributes to the scoring
// of records.
type FeatureQuery struct {
	// FieldBoosts is a list of FeatureFieldBoosts which contribute
	// to the feature score of a record in a query.
	FieldBoosts []FeatureFieldBoost
}

func (q FeatureQuery) proto() (*querypb.SearchRequest_FeatureQuery, error) {
	req := &querypb.SearchRequest_FeatureQuery{}
	if q.FieldBoosts != nil {
		afbs, err := featureFieldBoosts(q.FieldBoosts).proto()
		if err != nil {
			return nil, err
		}
		req.FieldBoosts = afbs
	}
	return req, nil
}

// Request is an API representation of a search request.
type Request struct {
	// Tracking configuration.
	Tracking Tracking

	// Filter to be applied (exclude records from results).
	Filter Filter

	// IndexQuery defines a query to be run against the search index.
	IndexQuery IndexQuery

	// FeatureQuery
	FeatureQuery FeatureQuery

	// Offset of results to return.
	Offset int

	// Limit is the number of results to return.
	Limit int

	// Ordering applied to results.
	Sort []Sort

	// Fields returned in results, if empty will return all fields.
	Fields []string

	// Aggregates is a set of Aggregates to run against a result set.
	Aggregates map[string]Aggregate

	// Transforms is a list of transforms to be applied to the query before it is run.
	Transforms []Transform
}

func (r Request) proto() (*pb.SearchRequest, error) {
	req := &querypb.SearchRequest{
		Offset: int32(r.Offset),
		Limit:  int32(r.Limit),
		Fields: r.Fields,
	}

	iq, err := r.IndexQuery.proto()
	if err != nil {
		return nil, err
	}
	req.IndexQuery = iq

	fq, err := r.FeatureQuery.proto()
	if err != nil {
		return nil, err
	}
	req.FeatureQuery = fq

	if r.Filter != nil {
		filter, err := r.Filter.proto()
		if err != nil {
			return nil, err
		}
		req.Filter = filter
	}

	if r.Sort != nil {
		sts, err := sorts(r.Sort).proto()
		if err != nil {
			return nil, err
		}
		req.Sort = sts
	}

	if r.Aggregates != nil {
		ags, err := aggregates(r.Aggregates).proto()
		if err != nil {
			return nil, err
		}
		req.Aggregates = ags
	}

	if r.Transforms != nil {
		transforms := make([]*querypb.Transform, 0, len(r.Transforms))
		for _, transform := range r.Transforms {
			transforms = append(transforms, &querypb.Transform{
				Identifier: string(transform),
			})
		}
		req.Transforms = transforms
	}

	tracking, err := r.Tracking.proto()
	if err != nil {
		return nil, err
	}

	return &pb.SearchRequest{
		Tracking:      tracking,
		SearchRequest: req,
	}, nil
}

// Body is weighted free text.
type Body struct {
	// Text to search for.
	Text string

	// Weight (significance) of text.
	Weight float64
}

func (w Body) proto() *querypb.Body {
	return &querypb.Body{
		Text:   w.Text,
		Weight: w.Weight,
	}
}

type terms []Term

func (ts terms) proto() []*querypb.Term {
	out := make([]*querypb.Term, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.proto())
	}
	return out
}

// Term is a scored term.
type Term struct {
	Value  string  // String representation of the field
	Field  string  // Meta field
	Pos    uint16  // Number of positive interactions
	Neg    uint16  // Number of negative interactions
	Weight float64 // Significance of term
	WOff   uint16  // Word offset
	POff   uint16  // Paragraph offset
}

func (t Term) proto() *querypb.Term {
	return &querypb.Term{
		Value:      t.Value,
		Field:      t.Field,
		Pos:        uint32(t.Pos),
		Neg:        uint32(t.Neg),
		Weight:     float64(t.Weight),
		WordOffset: uint32(t.WOff),
		ParaOffset: uint32(t.POff),
	}
}

func processResponse(pbResp *querypb.SearchResponse, tokens []*pb.Token) (*Results, error) {
	results := make([]Result, 0, len(pbResp.Results))
	for i, pbr := range pbResp.Results {
		values := make(map[string]interface{}, len(pbr.Values))
		for k, v := range pbr.Values {
			vv, err := valueFromProto(v)
			if err != nil {
				return nil, err
			}
			values[k] = vv
		}

		r := Result{
			Score:      pbr.Score,
			IndexScore: pbr.IndexScore,
			Values:     values,
		}

		if len(tokens) > i {
			switch t := tokens[i].Token.(type) {
			case *pb.Token_Click_:
				r.Tokens = map[string]interface{}{
					"click": t.Click.Token,
				}

			case *pb.Token_PosNeg_:
				r.Tokens = map[string]interface{}{
					"pos": t.PosNeg.Pos,
					"neg": t.PosNeg.Neg,
				}
			}
		}

		results = append(results, r)
	}

	d, err := time.ParseDuration(pbResp.Time)
	if err != nil {
		return nil, err
	}

	resp := &Results{
		Reads:        int(pbResp.Reads),
		TotalResults: int(pbResp.TotalResults),
		Time:         d,
		Results:      results,
	}

	if pbResp.Aggregates != nil {
		resp.Aggregates = processAggregatesResponse(pbResp.Aggregates)
	}
	return resp, nil
}

// Results is a collection of results from a Search.
type Results struct {
	// Reads is the total number of index values read.
	Reads int

	// TotalResults is the total number of results for the query.
	TotalResults int

	// Time taken to perform the query.
	Time time.Duration

	// Aggregates computed on the query results (see Aggregate).
	Aggregates map[string]interface{}

	// Results of the query.
	Results []Result
}

// Result is an individual query result.
type Result struct {
	// Values are field values of records.
	Values map[string]interface{}

	// Tokens contains any tokens associated with this Result.
	Tokens map[string]interface{}

	// Score is the overall score of this Result.
	Score float64

	// IndexScore is the index-matched score of this Result.
	IndexScore float64
}

// Sort is an interface satisfied by all types which produce sort config.
type Sort interface {
	proto() (*querypb.Sort, error)
}

const (
	sortTypeScore = iota
	sortTypeIndexScore
	sortTypeFeatureScore
)

// type sortByScore int
//
// func (s sortByScore) proto(*querypb.Sort, error) {
// 	switch s {
// 	case sortTypeScore:
// 		return &querypb.Sort{
// 			Type: &querypb.Sort_Score{
// 				Score: true,
// 			},
// 		}
// 	}
// }
//
// // SortByScore defines a sort order using the ranking score.
// //
// // This is not yet implemented.
// type SortByScore() Sort {
// 	return sortByScore(sortTypeScore)
// }
//
// // SortByIndexScore defines a sort order using the index ranking
// // score.
// //
// // This is not yet implemented.
// func SortByIndexScore() Sort {
// 	return sortByScore(sortTypeIndexScore)
// }
//
// // SortByFeatureScore defines a sort order using the feature ranking
// // score.
// //
// // This is not yet implemented.
// func SortByFeatureScore() Sort {
// 	return sortByFeatureScore(sortTypeFeatureScore)
// }

// SortByField defines a sort order using a field.
type SortByField string

func (s SortByField) proto() (*querypb.Sort, error) {
	var order querypb.Sort_Order
	field := string(s)
	if strings.HasPrefix(field, "-") {
		field = strings.TrimPrefix(field, "-")
		order = querypb.Sort_DESC
	}
	return &querypb.Sort{
		Type: &querypb.Sort_Field{
			Field: field,
		},
		Order: order,
	}, nil
}

type sorts []Sort

func (ss sorts) proto() ([]*querypb.Sort, error) {
	out := make([]*querypb.Sort, 0, len(ss))
	for _, s := range ss {
		x, err := s.proto()
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, nil
}

// Evaluate performs a search against a single record.
// func (c *Client) Evalutate(ctx context.Context, r *Request, rec Record) (*Results, error) {
// 	pr, err := r.proto()
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	prec, err := rec.proto()
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	pbResp, err := pb.NewQueryClient(c.ClientConn).Evaluate(c.newContext(ctx), )
// 	if err != nil {
// 		return nil, err
// 	}
// 	return processResponse(pbResp)
// }
