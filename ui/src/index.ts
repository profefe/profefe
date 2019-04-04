import {flamegraph} from 'd3-flame-graph';
import {heatmap} from 'd3-heatmap2';
import {select} from 'd3-selection';
import {scaleLinear} from 'd3-scale';

import 'd3-heatmap2/dist/d3-heatmap2.css';
import 'd3-flame-graph/dist/d3-flamegraph.css';

function renderHeatmap(data) {
    const chart = heatmap()
        .width(500)
        .colorScale(
            scaleLinear().range(['#F5F5DC', '#FF5032', '#E50914'])
        );

    select('#chart')
        .datum(data.values)
        .call(chart);
}

function renderFlamegraph(data) {
    const width = document.getElementById('chart').clientWidth;

    const chart = flamegraph()
        .width(width)
        .minFrameSize(1)
        .inverted(true);

    select('#chart')
        .datum(data.root)
        .call(chart);
}

const apiUrl = 'http://localhost:10100/ui/flamegraph?service=callback_worker&type=cpu&from=2019-03-13T00:00:00&to=2019-03-13T14:00:00';

fetch(apiUrl)
    .then((resp) => resp.json())
    .then(renderFlamegraph);
