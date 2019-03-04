package pprof

import "context"

type label struct {
	key   string
	value string
}

// LabelSet is a set of labels.
type LabelSet struct {
	list []label
}

// labelContextKey is the type of contextKeys used for profiler labels.
type labelContextKey struct{}

func labelValue(ctx context.Context) labelMap {
	labels, _ := ctx.Value(labelContextKey{}).(*labelMap)
	if labels == nil {
		return labelMap(nil)
	}
	return *labels
}

// labelMap is the representation of the label set held in the context type.
// This is an initial implementation, but it will be replaced with something
// that admits incremental immutable modification more efficiently.
type labelMap map[string]string
