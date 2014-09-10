//node server.js port1,port2
//node server.js 8142
var net = require('net');
var util = require('util');
var ports = process.argv[2].split('\,');
var metrics = new Metrics(10000);
var netBindFunc = function(c) { //'connection' listener
  metrics.incrConn();
  
  c.on('close', function() {
  	metrics.decConn();
  });
  function send(chunk){
  	metrics.incrReq(chunk.length);
  	c.write('pong\r\n');
  }
  c.on('data', send);
  c.on('error', function(e){
  	console.log('connection error ' + e);
  })
}
ports.forEach(function(port){
	var server = net.createServer(netBindFunc);
	server.listen(port, function() { //'listening' listener
	  console.log('server bound @' + port);
	});	
})



function Metrics(emitInterval){
	var startTime = new Date();
	var lastEmitTime = startTime;
	var connections = 0;
	var totalRequests = 0;
	var bytesReceived = 0;
	var requests = 0;
	var formattedMsg = 'uptime %ds, open connections %d, total requests %d, throughput %dKB/s, req/second %d';
	this.incrConn = function incrConn(){
		connections+=1;
	}
	this.decConn = function decConn(){
		connections-=1;
	}
	this.incrReq = function incrReq(sizeOfByte){
		bytesReceived+=sizeOfByte;
		totalRequests+=1;
		requests+=1;
	}
	function reset(){
		lastEmitTime = new Date();
		bytesReceived = 0;
		requests = 0;	
	}
	setInterval(function(){
		var totalUptime = parseInt((new Date() - startTime) / 1000);
		var uptime = parseInt((new Date() - lastEmitTime) / 1000);
		var throughput = (bytesReceived / 1024 / uptime).toPrecision(3);
		var reqPerSec = parseInt(requests / uptime);
		reset();
		console.log(util.format(formattedMsg, totalUptime, connections, totalRequests, throughput, reqPerSec));
	}, emitInterval);

}