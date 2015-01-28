$(document).ready(function() {
	$(chart_id).highcharts({
		chart: chart,
		title: title,
		rangeSelector: rangeSelector,
		series: series
	});
});
