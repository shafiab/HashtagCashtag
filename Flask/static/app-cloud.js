// D3 Word Cloud Implementation by Eric Coopey:
// http://bl.ocks.org/ericcoopey/6382449

var source = new EventSource('/stream');
var hash = {};
var width = 1200;
var height = 700;

//update hash (associative array) with incoming word and count
source.onmessage = function (event) {
  word = event.data.split("|")[0];
  count = event.data.split("|")[1];
  if(!skip(word)){
    hash[word]=count;
  }
};

//update function for visualization
var updateViz =  function(){
  //print console message
  console.log("cloudArray-1" + JSON.stringify(d3.entries(hash)));

  var frequency_list = d3.entries(hash);

  d3.layout.cloud().size([800, 300])
  .words(frequency_list)
  .rotate(0)
  .fontSize(function(d) { return d.value; })
  .on("end", draw)
  .start();
};

// run updateViz at #7000 milliseconds, or 7 second
window.setInterval(updateViz, 7000);

//clean list, can be added to word skipping bolt
var skipList = ["https","follow","1","2","please","following","followers","fucking","RT","the","at","a"];

var skip = function(tWord){
  for(var i=0; i<skipList.length; i++){
    if(tWord === skipList[i]){
      return true;
    }
  }
  return false;
};
