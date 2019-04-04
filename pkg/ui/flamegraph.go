package ui

import (
	"github.com/profefe/profefe/internal/pprof/graph"
	"github.com/profefe/profefe/internal/pprof/profile"
	"github.com/profefe/profefe/pkg/logger"
)

type Flamegraph struct {
	Root *CallTreeNode `json:"root"`
}

type CallTreeNode struct {
	Name     string          `json:"name"`
	FullName string          `json:"full_name"`
	Line     int             `json:"line"`
	Value    int64           `json:"value"`
	Children []*CallTreeNode `json:"children"`
}

func GetFlamegraph(log *logger.Logger, prof *profile.Profile) (*Flamegraph, error) {
	opt := &graph.Options{
		SampleValue: sampleValue,
		CallTree:    true,
	}
	g := graph.New(prof, opt)

	//log.Debugw("getFlamegraph", "graph", g)

	var nodes []*CallTreeNode
	nroots := 0
	rootValue := int64(0)
	nodeMap := make(map[*graph.Node]*CallTreeNode, len(g.Nodes))
	// Make all nodes and the map, collect the roots.
	for _, n := range g.Nodes {
		v := n.CumValue()
		node := &CallTreeNode{
			Name:     n.Info.Name,
			FullName: n.Info.PrintableName(),
			Line:     n.Info.Lineno,
			Value:    v,
		}
		nodes = append(nodes, node)
		if len(n.In) == 0 {
			nodes[nroots], nodes[len(nodes)-1] = nodes[len(nodes)-1], nodes[nroots]
			nroots++
			rootValue += v
		}
		nodeMap[n] = node
	}
	// Populate the child links.
	for _, n := range g.Nodes {
		node := nodeMap[n]
		for child := range n.Out {
			node.Children = append(node.Children, nodeMap[child])
		}
	}

	rootNode := &CallTreeNode{
		Name:     "root",
		FullName: "root",
		Value:    rootValue,
		Children: nodes[0:nroots],
	}

	fg := &Flamegraph{
		Root: rootNode,
	}
	return fg, nil
}

func sampleValue(v []int64) int64 {
	return v[len(v)-1]
}
