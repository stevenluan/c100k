//node client.js ip port1,port2 maxconn concurrent
//node client.js 10.64.242.5 8142,8143 1000 10
var net = require('net');

var async = require('async');
var maxConnectionCount = process.argv[4];
var ip = process.argv[2];

var ports = process.argv[3].split('\,');

var concurrent = process.argv[5];

var totalConnections = 0;

function makeConnection(host, port, cb){
	var client = net.connect({port: port, host: host},
	    function() { //'connect' listener
		  
	});
	var connected = false;
	client.on('error', function(e) {
	  console.log('client errored ' + e);
	  if(!connected) cb(e);
	});
	client.on('close', function() {
	  console.log('client closed');
	});
	
	client.on('connect', function(e) {
		connected = true;
		cb();
	});
}

var q = async.queue(function(task, callback){
	makeConnection(task.host, task.port, callback);	
}, concurrent);

q.drain = function() {
	console.log('all connections have been established, connection count is ' + totalConnections);
}
var i = 0;
while(i++<maxConnectionCount){
	var port = rr(ports, i);
	q.push({host:ip, port:port}, function(err){
		if(err) {
			console.log('connection tasked failed with ' + err);
			return;
		}
		totalConnections++;
		
	})
}
function rr(ports, index){
	return ports[index % ports.length];
}