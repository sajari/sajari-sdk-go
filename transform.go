package sajari

// Transform is a definition of a transformation applied to a Request
// which is applied before the Request is executed.
type Transform string

// Transforms commonly used when adding records.
const (
	// SplitStopStemIndexedFieldsTransform splits indexed fields into terms,
	// removes stop words and stems what remains.
	// This is the default setting for transforms.
	SplitStopStemIndexedFieldsTranform Transform = "split-stop-stem-indexed-fields"

	// StopStemmerTransform removes stop terms and stems terms.
	StopStemTransform Transform = "stop-stem"

	// SplitIndexFields splits index fields into terms.
	SplitIndexedFieldsTransform Transform = "split-indexed-fields"
)
