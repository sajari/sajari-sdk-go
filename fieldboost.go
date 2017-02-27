package sajari

import pb "github.com/sajari/protogen-go/sajari/engine/query/v1"

type fieldBoosts []FieldBoost

func (bs fieldBoosts) proto() ([]*pb.FieldBoost, error) {
	pbs := make([]*pb.FieldBoost, 0, len(bs))
	for _, b := range bs {
		pb, err := b.proto()
		if err != nil {
			return nil, err
		}
		pbs = append(pbs, pb)
	}
	return pbs, nil
}

type featureFieldBoosts []FeatureFieldBoost

func (bs featureFieldBoosts) proto() ([]*pb.SearchRequest_FeatureQuery_FieldBoost, error) {
	pbs := make([]*pb.SearchRequest_FeatureQuery_FieldBoost, 0, len(bs))
	for _, b := range bs {
		apb, err := b.proto()
		if err != nil {
			return nil, err
		}
		pbs = append(pbs, apb)
	}
	return pbs, nil
}

// FeatureFieldBoost is a wrapper which turns a FieldBoost into a feature.  See
// NewFeatureFieldBoost.
type FeatureFieldBoost struct {
	boost FieldBoost
	value float64
}

func (ab FeatureFieldBoost) proto() (*pb.SearchRequest_FeatureQuery_FieldBoost, error) {
	pbb, err := ab.boost.proto()
	if err != nil {
		return nil, err
	}

	return &pb.SearchRequest_FeatureQuery_FieldBoost{
		FieldBoost: pbb,
		Value:      ab.value,
	}, nil
}

// FeatureFieldBoost uses the normalised form of the FieldBoost b (computed internally) to count for
// a portion (value between 0 and 1) of the overall record score.
func NewFeatureFieldBoost(b FieldBoost, value float64) FeatureFieldBoost {
	return FeatureFieldBoost{
		boost: b,
		value: value,
	}
}

// FieldBoost is an interface satisfied by field-based boosting types.
//
// FieldBoosts are a way to influence the scoring of a record during a search based
// on the record's field data.  All boosts are all normalised internally (converted to
// a number between 0 and 1).
type FieldBoost interface {
	proto() (*pb.FieldBoost, error)
}

type filterFieldBoost struct {
	filter Filter
	value  float64
}

func (fb filterFieldBoost) proto() (*pb.FieldBoost, error) {
	pf, err := fb.filter.proto()
	if err != nil {
		return nil, err
	}

	return &pb.FieldBoost{
		FieldBoost: &pb.FieldBoost_Filter_{
			Filter: &pb.FieldBoost_Filter{
				Filter: pf,
				Value:  fb.value,
			},
		},
	}, nil
}

// FilterFieldBoost is a boost which is applied to records which satisfy the filter.
// Value must be greater than 0.  Records which match the filter will receive a boost
// of Value
func FilterFieldBoost(f Filter, value float64) FieldBoost {
	return &filterFieldBoost{
		filter: f,
		value:  value,
	}
}

// IntervalPoint is point-value pair used to construct an IntervalFieldBoost.
//
// It defines the boost value at a particular point in an interval.
type IntervalPoint struct {
	// Point is a field value.
	Point float64

	// Value of boost to assign at this point.
	Value float64
}

// IntervalFieldBoost represents an interval-based boost for numeric field values.
//
// An interval field boost is defined by a list of points with corresponding boost values.
// When a field value falls between between two IntervalPoints.Point values is computed linearly.
func IntervalFieldBoost(field string, values ...IntervalPoint) FieldBoost {
	return intervalFieldBoost{
		field:  field,
		values: values,
	}
}

type intervalFieldBoost struct {
	field  string
	values []IntervalPoint
}

type pointValues []IntervalPoint

func (pvs pointValues) proto() []*pb.FieldBoost_Interval_Point {
	out := make([]*pb.FieldBoost_Interval_Point, 0, len(pvs))
	for _, pv := range pvs {
		out = append(out, &pb.FieldBoost_Interval_Point{
			Point: pv.Point,
			Value: pv.Value,
		})
	}
	return out
}

func (db intervalFieldBoost) proto() (*pb.FieldBoost, error) {
	return &pb.FieldBoost{
		FieldBoost: &pb.FieldBoost_Interval_{
			Interval: &pb.FieldBoost_Interval{
				Field:  db.field,
				Points: pointValues(db.values).proto(),
			},
		},
	}, nil
}

// ElementFieldBoost represents an element-based boosting for repeated field values.
//
// The resulting boost is the proportion of elements in elts that are also in the field
// value.
func ElementFieldBoost(field string, elts []string) FieldBoost {
	return &elementFieldBoost{
		field: field,
		elts:  elts,
	}
}

type elementFieldBoost struct {
	field string   // Field containing stringArray.
	elts  []string // List of elements to match against.
}

func (eb elementFieldBoost) proto() (*pb.FieldBoost, error) {
	return &pb.FieldBoost{
		FieldBoost: &pb.FieldBoost_Element_{
			Element: &pb.FieldBoost_Element{
				Field: eb.field,
				Elts:  eb.elts,
			},
		},
	}, nil
}

// TextFieldBoost represents a text-based boosting for string fields.
//
// It compares the text gainst the record field using a bag-of-words model.
func TextFieldBoost(field string, text string) FieldBoost {
	return &textFieldBoost{
		field: field,
		text:  text,
	}
}

type textFieldBoost struct {
	field string // Field containing string data.
	text  string // Text to compare against.
}

func (tb textFieldBoost) proto() (*pb.FieldBoost, error) {
	return &pb.FieldBoost{
		FieldBoost: &pb.FieldBoost_Text_{
			Text: &pb.FieldBoost_Text{
				Field: tb.field,
				Text:  tb.text,
			},
		},
	}, nil
}
