package profile

// Well known labels.
const (
	LabelService    = "service"
	LabelID         = "id"
	LabelGeneration = "generation"
	LabelType       = "type"
)

type Label struct {
	Key, Value string
}

type Labels []Label

func (p Labels) Len() int           { return len(p) }
func (p Labels) Less(i, j int) bool { return p[i].Key < p[j].Key }
func (p Labels) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
