# ibmmq-wrapper
 
A light library-wrapper for [ibmmq](https://www.npmjs.com/package/ibmmq) with easy interface.

## Features

* configurable
* getting messages with processing in callback
* putting messages (commit/backout support)

---
## Requirements

## Installation

```bash
# Using NPM
$ npm install --save ibmmq-wrapper

# Using Yarn
$ yarn add ibmmq-wrapper
```

## Usage

```javascript
import * as IBMMQWrapper from 'ibmmq-wrapper';
//or
const IBMMQWrapper = require('ibmmq-wrapper');

//Pass your queue's params
const fooQueue = new IBMMQWrapper(connName, channelName, queueManager, queueName);

// Getting msg
fooQueue.getMessages((err, msg) => {
  if (err) {
    console.log(err);
  }
  console.log(msg);
});

// Getting msg with options
const getOpts = {
  options: ['MQGMO_NO_SYNCPOINT', 'MQGMO_WAIT'],
  matchOptions: 'MQMO_NONE',
  waitInterval: 'MQWI_UNLIMITED',
};

fooQueue.getMessages((err, msg) => {
  if (err) {
    console.log(err);
  }
  console.log(msg);
}, getOpts);

// Putting msg in queue
fooQueue.putMessage('test');

//Putting msg in queue with options
const putOpts = {
  options: ['MQPMO_NO_SYNCPOINT', 'MQPMO_NEW_MSG_ID', 'MQPMO_NEW_CORREL_ID'],
};

fooQueue.putMessage('new test', putOpts);
```

## API

### constructor(connName, channelName, queueManager, queueName)

```javascript
```

### getMessages(cb, opts)

```javascript
```

### putMessage(msg, opts)

```javascript
```

### Configuration

These are the available config options for:

* getMessages: https://www.ibm.com/support/knowledgecenter/SSB23S_1.1.0.2020/gtpc2/mqgmost.html
* putMessage: https://www.ibm.com/support/knowledgecenter/SSB23S_1.1.0.2020/gtpc2/mqpmost.html
