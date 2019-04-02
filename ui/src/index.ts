import {heatmap} from 'd3-heatmap2';
import {select} from 'd3-selection';
import {scaleLinear} from 'd3-scale';

import 'd3-heatmap2/dist/d3-heatmap2.css';

function render(data: any) {
    const chart = heatmap()
        .width(500)
        .colorScale(
            scaleLinear().range(['#F5F5DC', '#FF5032', '#E50914'])
        );

    select('#chart')
        .datum(data.values)
        .call(chart);
}

fetch('/data.json')
    .then((resp) => resp.json())
    .then(render);
