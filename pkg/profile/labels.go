package profile

import "sort"

type Label struct {
	Key, Value string
}

type Labels []Label

func LabelsFromMap(m map[string]interface{}) Labels {
	if len(m) == 0 {
		return nil
	}

	labels := make(Labels, len(m))

	for k, rawVal := range m {
		val, _ := rawVal.(string)
		labels = append(labels, Label{k, val})
	}

	sort.Sort(labels)

	return labels
}

func (p Labels) Len() int           { return len(p) }
func (p Labels) Less(i, j int) bool { return p[i].Key < p[j].Key }
func (p Labels) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
