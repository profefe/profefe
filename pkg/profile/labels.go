package profile

import (
	"net/url"
	"sort"
	"strings"
)

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
