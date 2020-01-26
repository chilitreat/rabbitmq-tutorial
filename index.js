require("dotenv").config()
var amqp = require('amqplib/callback_api');
var amqpConn = null;

function start() {
  amqp.connect(`${process.env.CLOUDAMQP_URL}?heartbeat=60`, function(err, conn) {
    if(err) {
      console.error("[AMQP]", err.message);
      return setTimeout(start, 1000);
    }
    conn.on("error", (err) => {
      if(err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message);
      }
    });
    conn.on("close", () => {
      console.error("[AMQP] reconnection");
      return setTimeout(start, 1000);
    });
    console.log("[AMQP] connected");
    amqpConn = conn;
    whenConnected();
  });
}

function whenConnected() {
  startPublicher();
  startWorcker();
}

var pubChannel = null
var offlinePubQueue = [];

function startPublicher() {
  amqpConn.createConfirmChannel((err, ch) => {
    if(closeOnErr(err)) return;
    ch.on("error", (err) => {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", () => {
      console.log("[AMQP] channle closed");
    });

    pubChannel = ch;
    while(true) {
      var m = offlinePubQueue.shift();
      if (!m) break;
      publish(m[0], m[1], m[2]);
    }
  });
}

function publish(exchange, routingKey, content) {
  try {
    pubChannel.publish(exchange, routingKey, content, { persistent: true },
      (err, ok) => {
        if (err) {
          console.error("[AMQP] publish", err);
          offlinePubQueue.push([exchange, routingKey, content]);
          pubChannel.connection.close();
        }
      });
  } catch (e) {
    console.error("[AMQP] publish", e.message);
    offlinePubQueue.push([exchange, routingKey, content]);
  }
}

function startWorcker() {
  amqpConn.createChannel((err, ch) => {
    if(closeOnErr(err)) return;
    ch.on("error", (err) => {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", () => {
      console.log("[AMQP] channel closed");
    });

    ch.prefetch(10);
    ch.assertQueue("jobs", {durable: true}, (err, _ok) => {
      if(closeOnErr(err)) return;
      ch.consume("jobs", processMsg, { noAck: false});
      console.log("Worker is started");
    });

    function processMsg(msg) {
      work(msg, (ok) => {
        try {
          if(ok) {
            ch.ack(msg);
          }else{
            ch.reject(msg, true);
          }
        } catch(e) {
          closeOnErr(e);
        }
      });
    }
  });
}

function work(msg, cb) {
  console.log("PDF processing of ", msg.content.toString());
  cb(true)
}

function closeOnErr(err) {
  if(!err) return false;
  console.error("[AMQP] error", err);
  amqpConn.close();
  return true;
}

setInterval(() => {
  publish("", "jobs", new Buffer("work work work"));
}, 1000);

start();