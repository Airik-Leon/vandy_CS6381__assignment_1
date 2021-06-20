// pubber.js
const zmq = require("zeromq");

// Broker Setup
const xpub = zmq.socket("xpub");

xpub.bind("tcp://*:5000");

const xsub = zmq.socket("xsub");

xsub.bind("tcp://*:4000");

zmq.proxy(xpub, xsub);
