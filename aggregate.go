package sajari

import pb "code.sajari.com/protogen-go/sajari/engine/query/v1"

// Aggregate is an interface which is implemented by all aggregate
// types in this package.
type Aggregate interface {
	proto() (*pb.Aggregate, error)
}

// aggregates is a mapping of aggregates to apply to the query.
type aggregates map[string]Aggregate

func (as aggregates) proto() (map[string]*pb.Aggregate, error) {
	m := make(map[string]*pb.Aggregate, len(as))
	for k, v := range as {
		a, err := v.proto()
		if err != nil {
			return nil, err
		}
		m[k] = a
	}
	return m, nil
}

// CountAggregate is an aggregate which counts unique field values.
func CountAggregate(field string) Aggregate {
	return &countAggregate{
		field: field,
	}
}

type countAggregate struct {
	field string
}

func (c countAggregate) proto() (*pb.Aggregate, error) {
	return &pb.Aggregate{
		Aggregate: &pb.Aggregate_Count_{
			Count: &pb.Aggregate_Count{
				Field: c.field,
			},
		},
	}, nil
}

// BucketAggregate is an aggregate which counts records which fall into
// a defined set of buckets.
func BucketAggregate(bs ...Bucket) Aggregate {
	return &bucketAggregate{
		buckets: bs,
	}
}

type bucketAggregate struct {
	buckets []Bucket
}

func (ba bucketAggregate) proto() (*pb.Aggregate, error) {
	pbuckets := make([]*pb.Aggregate_Bucket_Bucket, 0, len(ba.buckets))
	for _, b := range ba.buckets {
		pb, err := b.proto()
		if err != nil {
			return nil, err
		}
		pbuckets = append(pbuckets, pb)
	}

	return &pb.Aggregate{
		Aggregate: &pb.Aggregate_Bucket_{
			Bucket: &pb.Aggregate_Bucket{
				Buckets: pbuckets,
			},
		},
	}, nil
}

// Bucket is a container used to classify records in a result set (see BucketAggregate). A
// record is included in the bucket if it satisfies the filter.
type Bucket struct {
	// Name of the bucket.
	Name string

	// Filter that records must satisfy to be included in the bucket.
	Filter Filter
}

func (b Bucket) proto() (*pb.Aggregate_Bucket_Bucket, error) {
	filter, err := b.Filter.proto()
	if err != nil {
		return nil, err
	}
	return &pb.Aggregate_Bucket_Bucket{
		Name:   b.Name,
		Filter: filter,
	}, nil
}

// MaxAggregate computes the maximum value of a numeric field over a result set.
func MaxAggregate(field string) Aggregate {
	return maxAggregate(field)
}

type maxAggregate string

func (m maxAggregate) proto() (*pb.Aggregate, error) {
	return metricAggregateProto(string(m), pb.Aggregate_Metric_MAX)
}

// MinAggregate computes the minimum value of a numeric field over a result set.
func MinAggregate(field string) Aggregate {
	return minAggregate(field)
}

type minAggregate string

func (m minAggregate) proto() (*pb.Aggregate, error) {
	return metricAggregateProto(string(m), pb.Aggregate_Metric_MIN)
}

// AvgAggregate computes the average value of a numeric field over a result set.
func AvgAggregate(field string) Aggregate {
	return avgAggregate(field)
}

type avgAggregate string

func (m avgAggregate) proto() (*pb.Aggregate, error) {
	return metricAggregateProto(string(m), pb.Aggregate_Metric_AVG)
}

// SumAggregate computes the sum of a numeric field over a result set.
func SumAggregate(field string) Aggregate {
	return sumAggregate(field)
}

type sumAggregate string

func (m sumAggregate) proto() (*pb.Aggregate, error) {
	return metricAggregateProto(string(m), pb.Aggregate_Metric_SUM)
}

func metricAggregateProto(field string, ty pb.Aggregate_Metric_Type) (*pb.Aggregate, error) {
	return &pb.Aggregate{
		Aggregate: &pb.Aggregate_Metric_{
			Metric: &pb.Aggregate_Metric{
				Field: field,
				Type:  ty,
			},
		},
	}, nil
}

// BucketsResponse is a type returned from a query performing bucket aggregate.
type BucketsResponse map[string]BucketResponse

type BucketResponse struct {
	// Name of the bucket.
	Name string

	// Number of records.
	Count int
}

// CountResponse is a type returned from a query which has performed a count aggregate.
type CountResponse map[string]int

func processAggregatesResponse(pbResp map[string]*pb.AggregateResponse) map[string]interface{} {
	out := make(map[string]interface{}, len(pbResp))
	for k, v := range pbResp {
		switch v := v.AggregateResponse.(type) {
		case *pb.AggregateResponse_Count_:
			counts := make(map[string]int, len(v.Count.Counts))
			for ck, cv := range v.Count.Counts {
				counts[ck] = int(cv)
			}
			out[k] = CountResponse(counts)

		case *pb.AggregateResponse_Buckets_:
			buckets := make(map[string]BucketResponse, len(v.Buckets.Buckets))
			for bk, bv := range v.Buckets.Buckets {
				buckets[bk] = BucketResponse{
					Name:  bv.Name,
					Count: int(bv.Count),
				}
			}
			out[k] = BucketsResponse(buckets)

		case *pb.AggregateResponse_Metric_:
			out[k] = v.Metric.Value
		}
	}
	return out
}
