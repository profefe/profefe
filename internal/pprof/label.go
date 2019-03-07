package pprof

type Label struct {
	Key      string
	ValueStr string
	ValueNum int64
}

// LabelSet is a set of labels.
type LabelSet []Label
