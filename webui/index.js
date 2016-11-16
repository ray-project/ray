var express = require('express');
var app = express();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var redis = require("redis");
var task = require('./task.js');

if (process.argv.length > 2) {
  var port = process.argv[2];
  var sub = redis.createClient(port, {return_buffers: true});
  var db = redis.createClient(port, {return_buffers: true});
} else {
  var sub = redis.createClient({return_buffers: true});
  var db = redis.createClient({return_buffers: true});
}
const assert = require('assert');
var backlogtracker = function(backlog, channel) {


}
app.use(express.static(__dirname + '/client'));
app.get('/', function(req, res) {
  res.sendFile(__dirname + '/client/index.html');
});

sub.config("SET","notify-keyspace-events", "AKE");
sub.psubscribe("task_log:*");
sub.psubscribe("__keyspace@0__:obj:*");
sub.psubscribe("__keyspace@0__:Failures*");
sub.psubscribe("__keyspace@0__:RemoteFunction*")
io.on('connection', function(socket) {
  console.log('a user connected');
  socket.on('disconnect', function() {
    console.log('user disconnected');
  })
  sub.on('psubscribe', function (channel, count) {
    console.log("Subscribed");
  });

});

  backlogobject = [];
  backlogtask = [];
  backlogfailures = [];
  backlogremotefunction = [];
  var failureindex;
  db.llen("Failures", function(err,result){failureindex = result;});
  sub.on('pmessage', function (pattern, channel, message) {
    if (channel.toString().split(":")[0]==="__keyspace@0__") {
       console.log(channel.toString());
       switch (channel.toString().split(":")[1]) {
         case "Failures":
           db.lindex("Failures", failureindex++, function(err,result){backlogfailures.push({"functionname":result.toString().split(" ")[2].slice(5,-5), "error":result.toString()});});
         break;
         case "obj":
           db.smembers(channel.slice(15), function(err,result){console.log(result); backlogobject.push({"ObjectId":channel.slice(19).toString('hex'), "PlasmaStoreId":result[0].toString()})});
         break;
         case "RemoteFunction":
           db.hgetall(channel.slice(15), function(err,result){backlogremotefunction.push({"function_id":result.function_id.toString('hex'), "module":result.module.toString(), "name":result.name.toString()})});
         break;
         default: console.log(channel.toString());
         break;         
      }
    } else {
      backlogtask.push(task.parse_task_instance(message));
   }
  });



  setInterval(function () {
    if (backlogfailures.length > 0) {
      console.log("Sending ", backlogfailures.length, " objects on failure");
      console.log(backlogfailures);
      io.sockets.emit('failure', backlogfailures);
    }
    backlogfailures = [];
  }, 30);
  setInterval(function () {
    if (backlogobject.length > 0) {
      console.log("Sending ", backlogobject.length, " objects on object");
      console.log(backlogobject);
      io.sockets.emit('object', backlogobject);
    }
    backlogobject = [];
  }, 30);
  setInterval(function () {
    if (backlogtask.length > 0) {
      console.log("Sending ", backlogtask.length, " objects on task");
      io.sockets.emit('task', backlogtask);
    }
    backlogtask = [];
  }, 30);
  setInterval(function () {
    if (backlogremotefunction.length > 0) {
      console.log("Sending ", backlogremotefunction.length, " objects on remote");
      console.log(backlogremotefunction);
      io.sockets.emit('remote', backlogremotefunction);
    }
    backlogremotefunction = [];
  }, 30);
http.listen(3000, function(){
  console.log('listening on *:3000');
});
