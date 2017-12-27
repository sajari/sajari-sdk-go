package sajari

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"

	enginepb "code.sajari.com/protogen-go/sajari/engine"
	pb "code.sajari.com/protogen-go/sajari/engine/store/record"
	rpcpb "code.sajari.com/protogen-go/sajari/rpc"
)

// Field constants for internal fields.  NB: All field names prefixed
// with _ (underscore) are reserved for internal use.
const (
	// BodyField is the name of the internal field which is used to represent the
	// record body.
	BodyField = "_body"

	// IDField is the name of the internal identifier field which is added to
	// each record.
	IDField = "_id"
)

// ErrNoSuchRecord is returned when a requested record cannot be found.
var ErrNoSuchRecord = errors.New("sajari: no such record")

// Record is a set of key-value pairs.
type Record map[string]interface{}

// NewRecord creates a new Record with the given body and field values.
func NewRecord(body string, values map[string]interface{}) Record {
	d := make(Record, len(values)+1)
	for k, v := range values {
		d[k] = v
	}
	d[BodyField] = body
	return d
}

func valueFromProto(v *enginepb.Value) (interface{}, error) {
	switch v := v.Value.(type) {
	case *enginepb.Value_Single:
		return v.Single, nil

	case *enginepb.Value_Repeated_:
		return v.Repeated.Values, nil
	}
	return nil, fmt.Errorf("unexpected type: %T", v)
}

func pbSingleValue(x interface{}) (*enginepb.Value, error) {
	switch x := x.(type) {
	case int, uint, int64, uint64, int32, uint32, int16, uint16,
		int8, uint8, float32, float64, string, bool:
		return &enginepb.Value{
			Value: &enginepb.Value_Single{
				Single: fmt.Sprintf("%v", x),
			},
		}, nil
	}
	return nil, fmt.Errorf("expected single value, got %T", x)
}

func pbValueFromInterface(x interface{}) (*enginepb.Value, error) {
	switch x := x.(type) {
	case int, uint, int64, uint64, int32, uint32, int16, uint16,
		int8, uint8, float32, float64, string, bool:
		return &enginepb.Value{
			Value: &enginepb.Value_Single{
				Single: fmt.Sprintf("%v", x),
			},
		}, nil
	case time.Time:
		return &enginepb.Value{
			Value: &enginepb.Value_Single{
				Single: fmt.Sprintf("%v", x.Unix()),
			},
		}, nil
	}

	var vs []string
	switch x := x.(type) {
	case []string:
		vs = x

	case []int:
		vs = make([]string, 0, len(x))
		for _, v := range x {
			vs = append(vs, fmt.Sprintf("%v", v))
		}

	case []int64:
		vs = make([]string, 0, len(x))
		for _, v := range x {
			vs = append(vs, fmt.Sprintf("%v", v))
		}

	default:
		return nil, fmt.Errorf("unsupported value: %T", x)
	}

	return &enginepb.Value{
		Value: &enginepb.Value_Repeated_{
			Repeated: &enginepb.Value_Repeated{
				Values: vs,
			},
		},
	}, nil
}

type protoValues map[string]interface{}

func (p protoValues) proto() (map[string]*enginepb.Value, error) {
	values := make(map[string]*enginepb.Value, len(p))
	for k, v := range p {
		vv, err := pbValueFromInterface(v)
		if err != nil {
			return nil, err
		}
		values[k] = vv
	}
	return values, nil
}

func (r Record) proto() (*pb.Record, error) {
	values, err := protoValues(r).proto()
	if err != nil {
		return nil, err
	}
	return &pb.Record{
		Values: values,
	}, nil
}

type records []Record

func (rs records) proto() ([]*pb.Record, error) {
	pbrs := make([]*pb.Record, 0, len(rs))
	for _, r := range rs {
		pbr, err := r.proto()
		if err != nil {
			return nil, err
		}
		pbrs = append(pbrs, pbr)
	}
	return pbrs, nil
}

// Key is a unique identifier for a stored record.
type Key struct {
	field string
	value interface{}
}

// NewKey creates a new Key with a field and value.  Field must be marked as unique in
// the collection schema.
func NewKey(field string, value interface{}) *Key {
	return &Key{
		field: field,
		value: value,
	}
}

// String implements Stringer.
func (k *Key) String() string {
	if k == nil {
		return ""
	}
	return fmt.Sprintf("Key{Field: %q, Value: %q}", k.field, k.value)
}

type keys []*Key

func (ks keys) proto() ([]*enginepb.Key, error) {
	out := make([]*enginepb.Key, 0, len(ks))
	for _, k := range ks {
		pbk, err := k.proto()
		if err != nil {
			return nil, err
		}
		out = append(out, pbk)
	}
	return out, nil
}

func (k *Key) proto() (*enginepb.Key, error) {
	if k == nil {
		return nil, fmt.Errorf("empty key")
	}
	vv, err := pbSingleValue(k.value)
	if err != nil {
		return nil, fmt.Errorf("error marshalling key value: %v", err)
	}
	return &enginepb.Key{
		Field: k.field,
		Value: vv,
	}, nil
}

func keyFromProto(k *enginepb.Key) (*Key, error) {
	if k.Field == "" && k.Value == nil {
		return nil, nil
	}
	val, err := valueFromProto(k.Value)
	if err != nil {
		return nil, err
	}
	return NewKey(k.Field, val), nil
}

type pbKeys []*enginepb.Key

func (pbks pbKeys) keys() ([]*Key, error) {
	out := make([]*Key, 0, len(pbks))
	for _, pbKey := range pbks {
		key, err := keyFromProto(pbKey)
		if err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, nil
}

func multiErrorFromRecordStatusProto(status []*rpcpb.Status) error {
	out := make([]error, 0, len(status))
	empty := true
	for _, s := range status {
		var err error
		switch c := codes.Code(s.Code); c {
		case codes.OK:
			// Skip

		case codes.NotFound:
			err = ErrNoSuchRecord
			empty = false

		default:
			// For the moment we wrap the error into a grpc error.
			err = grpc.Errorf(c, s.Message)
			empty = false
		}
		out = append(out, err)
	}
	if empty {
		return nil
	}
	return MultiError(out)
}

// MultiError can be returned from calls which make multiple requests (see AddMulti, GetMulti, etc).
type MultiError []error

// Error implements error.
func (me MultiError) Error() string {
	n := 0
	msg := ""
	for _, e := range me {
		if e != nil {
			if n == 0 {
				msg = e.Error()
			}
			n++
		}
	}

	switch n {
	case 0:
		return "(0 errors)"
	case 1:
		return msg
	case 2:
		return fmt.Sprintf("%v (and 1 other error)", msg)
	}
	return fmt.Sprintf("%v (and %d other errors)", msg, n)
}

// Add adds a record to a collection, returning a key which can be used to retrieve the
// record.  If no transforms are specified then DefaultAddTransforms is used.
func (c *Client) Add(ctx context.Context, r Record, ts ...Transform) (*Key, error) {
	ks, err := c.AddMulti(ctx, []Record{r}, ts...)
	if err != nil {
		if me, ok := err.(MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return ks[0], nil
}

// DefaultAddTransforms is the default list of transforms which are used when adding records.
var DefaultAddTransforms = []Transform{
	SplitStopStemIndexedFieldsTranform,
}

// AddMulti adds records to the underlying collection, returning a list of Keys which can be used
// to retrieve the respective record.  If any of the adds fail then a MultiError will be returned
// with errors set in the respective indexes.
// If no transforms are specified then DefaultAddTransforms is used.
func (c *Client) AddMulti(ctx context.Context, rs []Record, ts ...Transform) ([]*Key, error) {
	pbrs, err := records(rs).proto()
	if err != nil {
		return nil, err
	}

	if len(ts) == 0 {
		ts = append(ts, DefaultAddTransforms...)
	}

	pbts := make([]*pb.Transform, 0, len(ts))
	for _, t := range ts {
		pbts = append(pbts, &pb.Transform{
			Identifier: string(t),
		})
	}

	pbks, err := pb.NewStoreClient(c.ClientConn).Add(c.newContext(ctx), &pb.Records{
		Records:    pbrs,
		Transforms: pbts,
	})
	if err != nil {
		return nil, err
	}

	keys, err := pbKeys(pbks.Keys).keys()
	if err != nil {
		return nil, err
	}
	return keys, multiErrorFromRecordStatusProto(pbks.Status)
}

type recordMutations []RecordMutation

func (rms recordMutations) proto() ([]*pb.MutateRequest_RecordMutation, error) {
	out := make([]*pb.MutateRequest_RecordMutation, 0, len(rms))
	for _, rm := range rms {
		rmpb, err := rm.proto()
		if err != nil {
			return nil, err
		}
		out = append(out, rmpb)
	}
	return out, nil
}

func (c *Client) MutateMulti(ctx context.Context, rms ...RecordMutation) error {
	rmspb, err := recordMutations(rms).proto()
	if err != nil {
		return err
	}

	resp, err := pb.NewStoreClient(c.ClientConn).Mutate(c.newContext(ctx), &pb.MutateRequest{
		RecordMutations: rmspb,
	})
	if err != nil {
		return err
	}
	return multiErrorFromRecordStatusProto(resp.Status)
}

// RecordMutation is a mutation to apply to a Record.
type RecordMutation struct {
	// Key identifies the record to mutate.
	Key *Key

	// FieldMutations to apply to the record.
	FieldMutations []FieldMutation
}

type fieldMutations []FieldMutation

func (fms fieldMutations) proto() ([]*pb.MutateRequest_RecordMutation_FieldMutation, error) {
	out := make([]*pb.MutateRequest_RecordMutation_FieldMutation, 0, len(fms))
	for _, fm := range fms {
		fmpb, err := fm.proto()
		if err != nil {
			return nil, err
		}
		out = append(out, fmpb)
	}
	return out, nil
}

func (m RecordMutation) proto() (*pb.MutateRequest_RecordMutation, error) {
	k, err := m.Key.proto()
	if err != nil {
		return nil, err
	}

	fms, err := fieldMutations(m.FieldMutations).proto()
	if err != nil {
		return nil, err
	}

	return &pb.MutateRequest_RecordMutation{
		Key:            k,
		FieldMutations: fms,
	}, nil
}

// Patch makes changes to a record identified by k in-place (where possible). To remove a value
// set the corresponding field name to nil in values map.
func (c *Client) Mutate(ctx context.Context, k *Key, m ...FieldMutation) error {
	err := c.MutateMulti(ctx, RecordMutation{k, m})
	if err != nil {
		if me, ok := err.(MultiError); ok {
			return me[0]
		}
	}
	return err
}

// Delete removes the record identified by key k.
func (c *Client) Delete(ctx context.Context, k *Key) error {
	err := c.DeleteMulti(ctx, []*Key{k})
	if err != nil {
		if me, ok := err.(MultiError); ok {
			return me[0]
		}
	}
	return err
}

// DeleteMulti removes the records identified by the keys k.  Returns non-nil error if there was
// a communication problem, but fails silently if any key doesn't have a corresponding record.
func (c *Client) DeleteMulti(ctx context.Context, ks []*Key) error {
	pbks, err := keys(ks).proto()
	if err != nil {
		return err
	}

	resp, err := pb.NewStoreClient(c.ClientConn).Delete(c.newContext(ctx), &pb.Keys{
		Keys: pbks,
	})
	if err != nil {
		return err
	}
	return multiErrorFromRecordStatusProto(resp.Status)
}

func recordFromProto(pbr *pb.Record) (Record, error) {
	d := make(Record)
	for k, v := range pbr.Values {
		vv, err := valueFromProto(v)
		if err != nil {
			return nil, err
		}
		d[k] = vv
	}
	return d, nil
}

type pbRecords []*pb.Record

func (pbrs pbRecords) records() ([]Record, error) {
	out := make([]Record, 0, len(pbrs))
	for _, pbr := range pbrs {
		d, err := recordFromProto(pbr)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

// Get returns the record identified by the Key.
func (c *Client) Get(ctx context.Context, k *Key) (Record, error) {
	resp, err := c.GetMulti(ctx, []*Key{k})
	if err != nil {
		if me, ok := err.(MultiError); ok {
			return nil, me[0]
		}
		return nil, err
	}
	return resp[0], nil
}

// Exists returns true iff a record identified by k exists or non-nil
// error if there was a problem completing the request.
func (c *Client) Exists(ctx context.Context, k *Key) (bool, error) {
	resp, err := c.ExistsMulti(ctx, []*Key{k})
	if err != nil {
		if me, ok := err.(MultiError); ok {
			return false, me[0]
		}
		return false, err
	}
	return resp[0], nil
}

// ExistsMulti checks whether records identified by keys exist.  Returns a slice
// of bool values indiciating the existence of each.
func (c *Client) ExistsMulti(ctx context.Context, k []*Key) ([]bool, error) {
	pbks, err := keys(k).proto()
	if err != nil {
		return nil, err
	}

	resp, err := pb.NewStoreClient(c.ClientConn).Exists(c.newContext(ctx), &pb.Keys{
		Keys: pbks,
	})
	if err != nil {
		return nil, err
	}

	out := make([]bool, 0, len(k))
	errs := make([]error, 0, len(k))
	empty := true
	for _, s := range resp.Status {
		var err error
		switch c := codes.Code(s.Code); c {
		case codes.OK:
			out = append(out, true)

		case codes.NotFound:
			out = append(out, false)

		default:
			err = grpc.Errorf(c, s.Message)
			empty = false
		}
		errs = append(errs, err)
	}

	if empty {
		return out, nil
	}
	return nil, MultiError(errs)
}

// GetMulti retrieves the records identified by the keys k.
func (c *Client) GetMulti(ctx context.Context, k []*Key) ([]Record, error) {
	pbks, err := keys(k).proto()
	if err != nil {
		return nil, err
	}

	resp, err := pb.NewStoreClient(c.ClientConn).Get(c.newContext(ctx), &pb.Keys{
		Keys: pbks,
	})
	if err != nil {
		return nil, err
	}

	docs, err := pbRecords(resp.Records).records()
	if err != nil {
		return nil, err
	}
	return docs, multiErrorFromRecordStatusProto(resp.Status)
}

// SetFields converts the map of field-value pairs into field mutations
// for use in Mutate.
func SetFields(m map[string]interface{}) []FieldMutation {
	out := make([]FieldMutation, 0, len(m))
	for k, v := range m {
		out = append(out, SetField(k, v))
	}
	return out
}

// FieldMutation is an interface satisfied by all field mutations defined
// in this package.
type FieldMutation interface {
	proto() (*pb.MutateRequest_RecordMutation_FieldMutation, error)
}

type setField struct {
	field string
	value interface{}
}

func (s setField) proto() (*pb.MutateRequest_RecordMutation_FieldMutation, error) {
	v, err := pbValueFromInterface(s.value)
	if err != nil {
		return nil, err
	}

	return &pb.MutateRequest_RecordMutation_FieldMutation{
		Field: s.field,
		Mutation: &pb.MutateRequest_RecordMutation_FieldMutation_Set{
			Set: v,
		},
	}, nil
}

// SetField is a FieldMutation which sets field to value.  If value is nil
// then this unsets field.
func SetField(field string, value interface{}) FieldMutation {
	return setField{field, value}
}
