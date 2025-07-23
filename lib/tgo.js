
const broker = require("mqtt");
const EventEmitter = require("events");
const { load_worksheet, combine_sheets, ProtocolBuffer, parse_protocol_buffer_schema, parse_converts, load_schema_codecs } = require("./utils");

class TGO extends EventEmitter {
    constructor(name, specialized_worksheet) {
        super();
        this.name = name;
        this.options = load_worksheet({},"/home/tamago/cmc_ws/xml/Book2.xlsx");  // main
        if (specialized_worksheet)
            this.options = load_worksheet(this.options, specialized_worksheet);
        this.options.mqtt.options.clientId = this.name;
        this.broker = broker.connect(`${this.options.mqtt.protocol}://${this.options.mqtt.address}:${this.options.mqtt.port}`);
        this.subscribers = {};
        this.publishers = {};
        this.intervals = {};
        parse_converts(this.options[name].convert);
    }
    connect() {
        this.broker.on("connect", () => { this.onConnect(); });
        this.broker.on("message", async (topic, message) => { await this.onMessage(topic, message); });
    }
    start() {
        for (const [rate,interval] of Object.entries(this.intervals))
            interval.interval = setInterval(() => { this.runInterval(rate); }, rate);
    }
    stop() {
        for (const interval of Object.values(this.intervals)) {
            if (!interval.interval)
                continue;
            clearInterval(interval.interval);
            delete interval.interval;
        }
    }
    loadVariables() {}
    loadClient() {}
    onConnect() {}
    async onMessage(topic,message) {
        const [subscriber, publisher, topicname, datatype] = topic.split("/");
        if (datatype === "request") {
            const request = JSON.parse(message.toString());
            if (request.datatype === "schema")
                this.publishSchema(topicname,request.subscriber);
            else if (request.datatype === "data")
                this.publishData(topicname,request.subscriber);
        } else {
            const sub = this.subscribers[publisher][topicname];
            if (!sub) return;
            if (datatype === "schema") {
                if (sub.schema.equals(message)) return;
                sub.schema = message;
                sub.decoder = load_schema_codecs(parse_protocol_buffer_schema(sub.schema.toString()),topicname).decoder;
            } else if (datatype === "data" && sub.decoder)
                await this.callbackSubscriber(publisher,topicname,sub.decoder(message),sub.args);
        }
    }
    subscribe(publisher,topicname,options={}) {
        if (!this.subscribers[publisher])
            this.subscribers[publisher] = {};
        options.schema = Buffer.of();
        this.subscribers[publisher][topicname] = options;
        this.broker.subscribe(`/${publisher}/${topicname}/data`);
        this.broker.subscribe(`/${publisher}/${topicname}/schema`);
        this.broker.subscribe(`${this.name}/${publisher}/${topicname}/data`);
        this.broker.subscribe(`${this.name}/${publisher}/${topicname}/schema`);
    }
    advertise(topicname,schema,encoder,options={}) {
        this.publishers[topicname] = options;
        this.publishers[topicname].schema = schema;
        this.publishers[topicname].encoder = encoder;
        this.broker.subscribe(`/${this.name}/${topicname}/request`);
        this.publishSchema(topicname);
    }
    async publishData(topicname,subscriber="") {
        const pub = this.publishers[topicname];
        if (!pub) return;
        //console.log(`data : ${subscriber}/${this.name}/${topicname}/data`);
        this.broker.publish(
            `${subscriber}/${this.name}/${topicname}/data`,
            pub.encoder(await this.preparePublishData(topicname,pub.args)),
            pub.options
        );
    }
    publishSchema(topicname,subscriber="") {
        const pub = this.publishers[topicname];
        if (!pub) return;
        console.log(`schema : ${subscriber}/${this.name}/${topicname}/schema : ${pub.schema}`);
        this.broker.publish(
            `${subscriber}/${this.name}/${topicname}/schema`,
            pub.schema,
            {retain: !Boolean(subscriber.length), qos: 1}
        );
    }

    runInterval(rate) {}

    async preparePublishData(topicname, args={}) { return {}; }
    async callbackSubscriber(publisher, topicname, data, args={}) { }
}

module.exports = TGO;
