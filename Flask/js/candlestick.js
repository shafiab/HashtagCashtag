$(function () {


    var data = {{ data|safe }}
    var text = {{ text|safe }}

    // create the chart
    $('#candleStickContainer').highcharts('StockChart', {


        title : {
            text : text
        },

        rangeSelector : {
            buttons : [
            {
                type : 'minute',
                count : 5,
                text : '5m'
            },
            {
                type : 'minute',
                count : 30,
                text : '30m'
            },
            {
                type : 'hour',
                count : 1,
                text : '1h'
            }, {
                type : 'day',
                count : 1,
                text : '1D'
            }, {
                type : 'all',
                count : 1,
                text : 'All'
            }],
            selected : 1,
            inputEnabled : false
        },


        series : [{
            type : 'candlestick',
            name : text,
            data : data,
            
        }]
    });
});