package sajari

import (
	"fmt"
	"strings"

	pb "code.sajari.com/protogen-go/sajari/engine/query/v1"
)

// Filter is an interface satisified by filter types that can defined in this package.
type Filter interface {
	proto() (*pb.Filter, error)
}

// FieldFilter creates a field-based filter.  The fieldOp argument must
// be a field name followed by optional space and then one of "=", "!=", ">", ">="
// "<", "<=", "~" (contains), "!~" (does not contain), "^" (prefix) or "$" (suffix).
//
// Filter which matches records where the field 'url' begins with "https://www.sajari.com":
//     FieldFilter("url ^", "https://www.sajari.com")
//
// Filter which matches records where the field 'name' contains "Sajari":
//     FieldFilter("name ~", "Sajari")
//
// Filter which matches records where the field 'count' is greater than or equal to 10:
//     FieldFilter("count >=", 10)
func FieldFilter(fieldOp string, value interface{}) Filter {
	field := strings.TrimRight(fieldOp, " <>=!~^$")
	return &fieldFilter{
		field: field,
		op:    strings.TrimSpace(fieldOp[len(field):]),
		value: value,
	}
}

type fieldFilter struct {
	op    string
	field string
	value interface{}
}

func (ff fieldFilter) proto() (*pb.Filter, error) {
	var op pb.Filter_Field_Operator
	switch ff.op {
	case "=":
		op = pb.Filter_Field_EQUAL_TO

	case "!=":
		op = pb.Filter_Field_NOT_EQUAL_TO

	case ">":
		op = pb.Filter_Field_GREATER_THAN

	case ">=":
		op = pb.Filter_Field_GREATER_THAN_OR_EQUAL_TO

	case "<":
		op = pb.Filter_Field_LESS_THAN

	case "<=":
		op = pb.Filter_Field_LESS_THAN_OR_EQUAL_TO

	case "~":
		op = pb.Filter_Field_CONTAINS

	case "!~":
		op = pb.Filter_Field_DOES_NOT_CONTAIN

	case "^":
		op = pb.Filter_Field_HAS_PREFIX

	case "$":
		op = pb.Filter_Field_HAS_SUFFIX

	default:
		return nil, fmt.Errorf("invalid field filter operator: %v", ff.op)
	}

	value, err := pbValueFromInterface(ff.value)
	if err != nil {
		return nil, fmt.Errorf("error marshalling value: %v", err)
	}

	return &pb.Filter{
		Filter: &pb.Filter_Field_{
			Field: &pb.Filter_Field{
				Field:    ff.field,
				Operator: op,
				Value:    value,
			},
		},
	}, nil
}

// enumeration of combination filter operators.
type combFilterOp int

const (
	combFilterOpAll  combFilterOp = iota // All filters must be satisfied.
	combFilterOpAny                      // Any filter must be satisfied.
	combFilterOpOne                      // Only one of the filters must be satisfied
	combFilterOpNone                     // None of the filters must be satisfied.
)

func newCombFilter(op combFilterOp, filters []Filter) Filter {
	return &combFilter{
		op:      op,
		filters: filters,
	}
}

// AllFilters returns a filter which matches documensts that satisfy all of the supplied
// filters.  Equivalent to AND.
func AllFilters(filters ...Filter) Filter {
	return newCombFilter(combFilterOpAll, filters)
}

// AnyFilter returns a filter which matches records that satisfy any of the given
// filters.  Equivalent to OR.
func AnyFilter(filters ...Filter) Filter {
	return newCombFilter(combFilterOpAny, filters)
}

// OneOfFilters returns a filter which matches records that satisfy only one of the given
// filters.  Equivalent to XOR.
func OneOfFilters(filters ...Filter) Filter {
	return newCombFilter(combFilterOpOne, filters)
}

// NoneOfFilters returns a filter which matches records that do not satisfy any of the
// given filters.  Equivalent to NAND.
func NoneOfFilters(filters ...Filter) Filter {
	return newCombFilter(combFilterOpNone, filters)
}

type combFilter struct {
	op      combFilterOp
	filters []Filter
}

func (cf combFilter) proto() (*pb.Filter, error) {
	var op pb.Filter_Combinator_Operator
	switch cf.op {
	case combFilterOpAll:
		op = pb.Filter_Combinator_ALL

	case combFilterOpAny:
		op = pb.Filter_Combinator_ANY

	case combFilterOpOne:
		op = pb.Filter_Combinator_ONE

	case combFilterOpNone:
		op = pb.Filter_Combinator_NONE

	default:
		return nil, fmt.Errorf("invalid combinator operator: %v", cf.op)
	}

	pfs := make([]*pb.Filter, 0, len(cf.filters))
	for _, f := range cf.filters {
		pf, err := f.proto()
		if err != nil {
			return nil, err
		}
		pfs = append(pfs, pf)
	}

	return &pb.Filter{
		Filter: &pb.Filter_Combinator_{
			Combinator: &pb.Filter_Combinator{
				Operator: op,
				Filters:  pfs,
			},
		},
	}, nil
}

// GeoFilterRegion is an enumeration of region values for specifying regions
// in GeoFilters
type GeoFilterRegion int

// Constants for use with GeoFilter.Region.
const (
	// GeoFilterInside is used to configure a geo filter to be
	// applied to all points within the radius.
	GeoFilterInside GeoFilterRegion = iota

	// GeoFilterOutside is used to contigure a geo boost to be
	// applied to all points outside the radius.
	GeoFilterOutside
)

// GeoFilter is a geo-based boost for records with numeric fields containing latitude/longitude.
//
//    // Construct a geo-filter on fields "lat" and "lng" which define a location
//    // within 10km of Sydney (33.8688° S, 151.2093° E).
//    GeoFilter("lat", "lng", -33.8688, 151.2093, 10.00, GeoFilterInside)
func GeoFilter(fieldLat, fieldLng string, lat, lng, radius float64, region GeoFilterRegion) Filter {
	return &geoFilter{
		fieldLat: fieldLat,
		fieldLng: fieldLng,
		lat:      lat,
		lng:      lng,
		radius:   radius,
		region:   region,
	}
}

type geoFilter struct {
	fieldLat string          // Field containing latitude.
	fieldLng string          // Field containing longitude.
	lat      float64         // Target latitude.
	lng      float64         // Target longitude.
	radius   float64         // Radius of matching border.
	region   GeoFilterRegion // Region for matching points.
}

func (gb geoFilter) proto() (*pb.Filter, error) {
	var region pb.Filter_Geo_Region
	switch gb.region {
	case GeoFilterInside:
		region = pb.Filter_Geo_INSIDE

	case GeoFilterOutside:
		region = pb.Filter_Geo_OUTSIDE

	default:
		return nil, fmt.Errorf("geo filter: invalid region '%v'", gb.region)
	}

	return &pb.Filter{
		Filter: &pb.Filter_Geo_{
			Geo: &pb.Filter_Geo{
				FieldLat: gb.fieldLat,
				FieldLng: gb.fieldLng,
				Lat:      gb.lat,
				Lng:      gb.lng,
				Radius:   gb.radius,
				Region:   region,
			},
		},
	}, nil
}
