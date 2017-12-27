package sajari

import pb "code.sajari.com/protogen-go/sajari/engine/query/v1"

type instanceBoosts []InstanceBoost

func (bs instanceBoosts) proto() ([]*pb.InstanceBoost, error) {
	pbs := make([]*pb.InstanceBoost, 0, len(bs))
	for _, b := range bs {
		pb, err := b.proto()
		if err != nil {
			return nil, err
		}
		pbs = append(pbs, pb)
	}
	return pbs, nil
}

// InstanceBoost is an interface satisfied by instance-based boosting types defined
// in this package.
//
// InstanceBoosts are a way to influence the scoring of a record during a search
// by changing the importance of term instances in indexed records.
type InstanceBoost interface {
	proto() (*pb.InstanceBoost, error)
}

type fieldInstanceBoost struct {
	field string
	value float64
}

func (fb fieldInstanceBoost) proto() (*pb.InstanceBoost, error) {
	return &pb.InstanceBoost{
		InstanceBoost: &pb.InstanceBoost_Field_{
			Field: &pb.InstanceBoost_Field{
				Field: fb.field,
				Value: fb.value,
			},
		},
	}, nil
}

// FieldInstanceBoost is a boost applied to index terms which originate in
// a particular field.
func FieldInstanceBoost(field string, value float64) InstanceBoost {
	return &fieldInstanceBoost{
		field: field,
		value: value,
	}
}

type scoreInstanceBoost struct {
	minCount  int
	threshold float64
}

func (sib scoreInstanceBoost) proto() (*pb.InstanceBoost, error) {
	return &pb.InstanceBoost{
		InstanceBoost: &pb.InstanceBoost_Score_{
			Score: &pb.InstanceBoost_Score{
				MinCount:  uint32(sib.minCount),
				Threshold: sib.threshold,
			},
		},
	}, nil
}

// ScoreInstanceBoost is a boost applied to index terms which have interaction
// data set.  For an instance score boost to take effect, the instance must have received
// at least minCount score updates (i.e. count).
// If an item is performing as it should then its score will be 1.
// If the score is below threshold (0 < threshold < 1) then the score will be applied.
func ScoreInstanceBoost(minCount int, threshold float64) InstanceBoost {
	return &scoreInstanceBoost{
		minCount:  minCount,
		threshold: threshold,
	}
}
