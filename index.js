import mq from "ibmmq";

class IBMMQWrapper {
  constructor(connName, channelName, queueManager, queueName) {
    this.MQC = mq.MQC;
    this.connName = connName;
    this.channelName = channelName;
    this.queueManager = queueManager;
    this.queueName = queueName;
    this.connectionHandle;
    this.queueHandle;
    // Params for Connect
    this.cd = new mq.MQCD();
    this.cno = new mq.MQCNO();
    this.cd.ConnectionName = connName;
    this.cd.ChannelName = channelName;
    this.cno.ClientConn = this.cd;
    this.md = new mq.MQMD();
  }

  getMessages(cb, opts) {
    mq.ConnxPromise(this.queueManager, this.cno)
      .then((hConn) => {
        console.log("MQCONN to %s successful ", this.queueManager);
        const od = new mq.MQOD();
        od.ObjectName = this.queueName;
        od.ObjectType = this.MQC.MQOT_Q;
        const openOptions = this.MQC.MQOO_INPUT_AS_Q_DEF;
        return mq.OpenPromise(hConn, od, openOptions);
      })
      .then((hObj) => {
        console.log("MQOPEN of %s successful", this.queueName);
        this.queueHandle = hObj;
        const gmo = new mq.MQGMO();
        if (opts) {
          gmo.Options = this.MQC[opts.options[0]] || this.MQC[opts.options[1]];
          gmo.MatchOptions = this.MQC[opts.matchOptions];
          gmo.WaitInterval = this.MQC[opts.waitInterval];
        }
        mq.Get(hObj, this.md, gmo, (err, hObj, gmo, md, buff, hConn) => {
          this.connectionHandle = hConn;
          cb(err, buff);
        });
      })
      .catch((err) => console.log(this.formatErr(err)));
  }

  putMessage(msg, opts) {
    mq.ConnxPromise(this.queueManager, this.cno)
      .then((hConn) => {
        console.log("MQCONN to %s successful ", this.queueManager);
        this.connectionHandle = hConn;
        const od = new mq.MQOD();
        od.ObjectName = this.queueName;
        od.ObjectType = this.MQC.MQOT_Q;
        const openOptions = this.MQC.MQOO_OUTPUT;
        return mq.OpenPromise(hConn, od, openOptions);
      })
      .then((hObj) => {
        console.log("MQOPEN of %s successful", this.queueName);
        this.queueHandle = hObj;
        const pmo = new mq.MQPMO();
        if (opts) {
          pmo.Options =
            this.MQC[opts.options[0]] ||
            this.MQC[opts.options[1]] ||
            this.MQC[opts.options[2]];
        }
        mq.PutPromise(this.queueHandle, this.md, pmo, msg);
      })
      .then(() => mq.ClosePromise(this.queueHandle, 0))
      .then(() => mq.DiscPromise(this.connectionHandle))
      .then(() => console.log("Done"))
      .catch((err) => console.log(this.formatErr(err)));
  }

  commit() {
    mq.Cmit(this.connectionHandle);
  }

  backout() {
    mq.Back(this.connectionHandle);
  }

  formatErr(err) {
    if (err) {
      return `MQ call failed at ${err.message}`;
    }
    return "MQ call successful";
  }
}

module.exports = IBMMQWrapper;
