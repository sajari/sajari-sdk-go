package sajari

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"

	pb "github.com/sajari/protogen-go/sajari/engine/schema"
	rpcpb "github.com/sajari/protogen-go/sajari/rpc"
)

// Schema returns the schema (list of fields) for the collection.
func (c *Client) Schema() *Schema {
	return &Schema{
		c: c,
	}
}

// Schema provides methods for managing collection schemas.  Use Client.Schema to create
// one for a collection.
type Schema struct {
	c *Client
}

// Fields returns the fields in the collection.
func (s *Schema) Fields(ctx context.Context) ([]Field, error) {
	schema, err := pb.NewSchemaClient(s.c.ClientConn).GetFields(s.c.newContext(ctx), &rpcpb.Empty{})
	if err != nil {
		return nil, err
	}

	out := make([]Field, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		t, err := typeFromProto(f.Type)
		if err != nil {
			return nil, err
		}

		out = append(out, Field{
			Name:        f.Name,
			Description: f.Description,
			Type:        t,
			Repeated:    f.Repeated,
			Required:    f.Required,
			Indexed:     f.Indexed,
			Unique:      f.Unique,
		})
	}
	return out, nil
}

// Field represents a meta field which can be assigned in a collection record.
type Field struct {
	// Name is the name used to identify the field.
	Name string

	// Description is a description of the field.
	Description string

	// Type defines the type of the field.
	Type Type

	// Repeated indicates that this field can hold a list of values.
	Repeated bool

	// Required indicates that this field should always be set on all records.
	Required bool

	// Indexed indicates that the field should be indexed.  This is only valid for
	// String or StringArray fields (see TypeString, TypeStringArray).
	Indexed bool

	// Unique indicates that the field is unique (and this will
	// be encoforced when new records are added).  Unique fields can
	// be used to retrieve/delete records.
	Unique bool
}

func (f Field) proto() (*pb.Field, error) {
	t, err := f.Type.proto()
	if err != nil {
		return nil, err
	}
	return &pb.Field{
		Name:        f.Name,
		Description: f.Description,
		Type:        t,
		Repeated:    f.Repeated,
		Required:    f.Required,
		Indexed:     f.Indexed,
		Unique:      f.Unique,
	}, nil
}

type fields []Field

func (fs fields) proto() (*pb.Fields, error) {
	pbfs := make([]*pb.Field, 0, len(fs))
	for _, f := range fs {
		pbf, err := f.proto()
		if err != nil {
			return nil, err
		}
		pbfs = append(pbfs, pbf)
	}

	return &pb.Fields{
		Fields: pbfs,
	}, nil
}

func typeFromProto(t pb.Field_Type) (Type, error) {
	switch t {
	case pb.Field_STRING:
		return TypeString, nil

	case pb.Field_INTEGER:
		return TypeInteger, nil

	case pb.Field_FLOAT:
		return TypeFloat, nil

	case pb.Field_BOOLEAN:
		return TypeBoolean, nil

	case pb.Field_TIMESTAMP:
		return TypeTimestamp, nil

	}
	return TypeString, fmt.Errorf("unknown type: '%v'", string(t))
}

// Type defines field data types.
type Type string

const (
	TypeString    Type = "STRING"
	TypeInteger   Type = "INTEGER"
	TypeFloat     Type = "FLOAT"
	TypeBoolean   Type = "BOOLEAN"
	TypeTimestamp Type = "TIMESTAMP"
)

func (t Type) proto() (pb.Field_Type, error) {
	switch t {
	case TypeString:
		return pb.Field_STRING, nil

	case TypeInteger:
		return pb.Field_INTEGER, nil

	case TypeFloat:
		return pb.Field_FLOAT, nil

	case TypeBoolean:
		return pb.Field_BOOLEAN, nil

	case TypeTimestamp:
		return pb.Field_TIMESTAMP, nil
	}
	return pb.Field_STRING, fmt.Errorf("unknown type: '%v'", string(t))
}

func multiErrorFromSchemaStatusProto(status []*rpcpb.Status) error {
	out := make([]error, 0, len(status))
	empty := true
	for _, s := range status {
		var err error
		switch c := codes.Code(s.Code); c {
		case codes.OK:
			// Skip

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

// Add adds Fields to the collection schema.
func (s *Schema) Add(ctx context.Context, fs ...Field) error {
	pbfs, err := fields(fs).proto()
	if err != nil {
		return err
	}
	resp, err := pb.NewSchemaClient(s.c.ClientConn).AddFields(s.c.newContext(ctx), pbfs)
	if err != nil {
		return err
	}
	return multiErrorFromSchemaStatusProto(resp.Status)
}

// MutateField mutates a field identifier by name.  Each mutation is performed in the order
// in which it is specified.  If any fail, then the rest are ignored.
func (s *Schema) MutateField(ctx context.Context, name string, muts ...Mutation) error {
	pbMuts, err := mutations(muts).proto()
	if err != nil {
		return err
	}

	resp, err := pb.NewSchemaClient(s.c.ClientConn).MutateField(ctx, &pb.MutateFieldRequest{
		Name:      name,
		Mutations: pbMuts,
	})
	if err != nil {
		return err
	}
	return multiErrorFromSchemaStatusProto(resp.Status)
}

type mutations []Mutation

func (ms mutations) proto() ([]*pb.MutateFieldRequest_Mutation, error) {
	out := make([]*pb.MutateFieldRequest_Mutation, 0, len(ms))
	for _, m := range ms {
		x, err := m.proto()
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, nil
}

// NameMutation creates a schema field mutation which changes the name of a field.
func NameMutation(name string) Mutation {
	return nameMutation(name)
}

type nameMutation string

func (n nameMutation) proto() (*pb.MutateFieldRequest_Mutation, error) {
	return &pb.MutateFieldRequest_Mutation{
		Mutation: &pb.MutateFieldRequest_Mutation_Name{
			Name: string(n),
		},
	}, nil
}

// TypeMutation creates a schema field mutation which changes the type of a field.
func TypeMutation(ty Type) Mutation {
	return typeMutation(ty)
}

type typeMutation Type

func (t typeMutation) proto() (*pb.MutateFieldRequest_Mutation, error) {
	ty, err := Type(t).proto()
	if err != nil {
		return nil, err
	}
	return &pb.MutateFieldRequest_Mutation{
		Mutation: &pb.MutateFieldRequest_Mutation_Type{
			Type: ty,
		},
	}, nil
}

// UniqueMutation creates a schema field mutation which changes the unique constraint on a field.
func UniqueMutation(unique bool) Mutation {
	return uniqueMutation(unique)
}

type uniqueMutation bool

func (u uniqueMutation) proto() (*pb.MutateFieldRequest_Mutation, error) {
	return &pb.MutateFieldRequest_Mutation{
		Mutation: &pb.MutateFieldRequest_Mutation_Unique{
			Unique: bool(u),
		},
	}, nil
}

// IndexedMutation creates a schema field mutation which changes the indexed property on a field.
func IndexedMutation(indexed bool) Mutation {
	return indexedMutation(indexed)
}

type indexedMutation bool

func (i indexedMutation) proto() (*pb.MutateFieldRequest_Mutation, error) {
	return &pb.MutateFieldRequest_Mutation{
		Mutation: &pb.MutateFieldRequest_Mutation_Indexed{
			Indexed: bool(i),
		},
	}, nil
}

// RepeatedMutation creates a schema field mutation which changes the repeated property on a field.
func RepeatedMutation(repeated bool) Mutation {
	return repeatedMutation(repeated)
}

type repeatedMutation bool

func (u repeatedMutation) proto() (*pb.MutateFieldRequest_Mutation, error) {
	return &pb.MutateFieldRequest_Mutation{
		Mutation: &pb.MutateFieldRequest_Mutation_Repeated{
			Repeated: bool(u),
		},
	}, nil
}

// RequiredMutation creates a schema field mutation which changes the required constraint on a field.
func RequiredMutation(required bool) Mutation {
	return requiredMutation(required)
}

type requiredMutation bool

func (u requiredMutation) proto() (*pb.MutateFieldRequest_Mutation, error) {
	return &pb.MutateFieldRequest_Mutation{
		Mutation: &pb.MutateFieldRequest_Mutation_Required{
			Required: bool(u),
		},
	}, nil
}

// Mutation is an interface which is satisfied by schema field mutations.
type Mutation interface {
	proto() (*pb.MutateFieldRequest_Mutation, error)
}
