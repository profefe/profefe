package profile

import (
	"net/url"
	"sort"
	"strings"
)

type Label struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Labels []Label

func LabelsFromMap(m map[string]interface{}) Labels {
	if len(m) == 0 {
		return nil
	}

	labels := make(Labels, 0, len(m))

	for k, rawVal := range m {
		if k == "" {
			continue
		}
		val, _ := rawVal.(string)
		labels = append(labels, Label{k, val})
	}

	sort.Sort(labels)

	return labels
}

func (labels Labels) Equal(labels2 Labels) bool {
	if len(labels) != len(labels2) {
		return false
	}

	labelsSet := make(map[string][]Label, len(labels))
	for _, label := range labels {
		labelsSet[label.Key] = append(labelsSet[label.Key], label)
	}

	for _, label := range labels2 {
		v, ok := labelsSet[label.Key]
		if !ok {
			return false
		}
		ok = false
		for _, label2 := range v {
			if label.Value == label2.Value {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}

	return true
}

// XXX(narqo): doesn't cover the case where Labels have multiple values of a key.
func (labels Labels) Include(labels2 Labels) bool {
	if len(labels2) == 0 {
		return true
	}

	if len(labels) == 0 {
		return false
	}

	kvset := make(map[string]string, len(labels))
	for _, label := range labels {
		kvset[label.Key] = label.Value
	}

	for _, label := range labels2 {
		v, ok := kvset[label.Key]
		if !ok {
			return false
		}
		if label.Value != v {
			return false
		}
	}

	return true
}

func (labels Labels) Add(labels2 Labels) Labels {
	if labels == nil {
		return labels2
	} else if labels2 == nil {
		return labels
	}

	labelsIdx := make(map[Label]struct{}, len(labels))
	for _, label := range labels {
		labelsIdx[label] = struct{}{}
	}

	ret := make([]Label, len(labels), len(labels)+len(labels2))
	copy(ret, labels)

	for _, label2 := range labels2 {
		_, ok := labelsIdx[label2]
		if !ok {
			ret = append(ret, label2)
		}
	}

	return ret
}

func (labels *Labels) FromString(s string) (err error) {
	if s == "" {
		return nil
	}

	var chunk string
	for s != "" {
		chunk, s = split2(s, ',')
		key, val := split2(chunk, '=')

		key, err = url.QueryUnescape(strings.TrimSpace(key))
		if err != nil {
			return err
		}
		if key == "" {
			continue
		}
		val, err = url.QueryUnescape(strings.TrimSpace(val))
		if err != nil {
			return err
		}
		*labels = append(*labels, Label{key, val})
	}

	if len(*labels) != 0 {
		sort.Sort(labels)
	}

	return nil
}

func split2(s string, ch byte) (s1, s2 string) {
	for i := 0; i < len(s); i++ {
		if s[i] == ch {
			return s[:i], s[i+1:]
		}
	}
	return s, ""
}

func (labels Labels) String() string {
	var buf strings.Builder
	for i, label := range labels {
		if i != 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(label.Key)
		buf.WriteByte('=')
		buf.WriteString(label.Value)
	}
	return buf.String()
}

func (labels Labels) Len() int           { return len(labels) }
func (labels Labels) Less(i, j int) bool { return labels[i].Key < labels[j].Key }
func (labels Labels) Swap(i, j int)      { labels[i], labels[j] = labels[j], labels[i] }
