const broker = require("mqtt");
const EventEmitter = require("events");
const { load_worksheet, parse_protocol_buffer_schema, parse_converts, load_schema_codecs } = require("./utils");


class TGO extends EventEmitter {
    constructor(sys_argv, xlsx_config) {
        super();
        console.log(sys_argv);
        console.log(xlsx_config);
        this.name = sys_argv.node_name;
        this.options = load_worksheet({}, xlsx_config.default.path, xlsx_config.default.name);  // main
        if (xlsx_config.specific && xlsx_config.specific.path && xlsx_config.specific.name)
            this.options = load_worksheet(this.options, xlsx_config.specific.path, xlsx_config.specific.name);
        this.options.mqtt.options.clientId = this.name;
        if (this.options.mqtt.options.will)
            this.options.mqtt.options.will = {
                topic: `/${this.options.mqtt.options.clientId}/keepalive`,
                payload: Buffer.of(0),
                qos: 1,
                retain: true
            };
        this.subscribers = {};
        this.publishers = {};
        this.intervals = {};
        if (this.options[this.name] && this.options[this.name].convert)
            parse_converts(this.options[this.name].convert);
        this.broker = broker.connect(`${this.options.mqtt.protocol}://${this.options.mqtt.address}:${this.options.mqtt.port}`, this.options.mqtt.options);
        //this.broker = broker.connect(`${this.options.mqtt.protocol}://${this.options.mqtt.address}:${this.options.mqtt.port}`, this.options.mqtt.options);
        this.broker.on("connect", () => { this.onConnect(); });
        // TODO : Maybe i should remove the "async await" from here, i shouldn't care about it
        this.broker.on("message", async (topic, message) => { await this.onMessage(topic, message); });
    }
    connect() {}
    
    // TODO : Fix this interval to work without "setInterval", cause it sucks hard
    start() {
        for (const [rate,interval] of Object.entries(this.intervals)) {
            if (interval._running) continue;
            interval._running = true;
            interval._interval = this._setInterval(interval, Number(rate));
        }
    }
    stop() { for (const interval of Object.values(this.intervals)) interval._running = false; }
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
                await this.publishData(topicname,request.subscriber);
        } else {
            const sub = this.subscribers[publisher][topicname];
            if (!sub) return;
            if (datatype === "schema") {
                if (sub.schema.equals(message)) return;
                sub.schema = message;
                sub.decoder = load_schema_codecs(parse_protocol_buffer_schema(sub.schema.toString()),topicname).decoder;
            } else if (datatype === "data" && sub.decoder)
                // TODO : Maybe i should remove the "async await" from here, i shouldn't care about it
                await this.callbackSubscriber(publisher,topicname,sub.decoder(message),sub.args);
        }
    }
    subscribe(publisher,topicname,options={}) {
        if (!this.subscribers[publisher])
            this.subscribers[publisher] = {};
        options.schema = Buffer.of();
        this.subscribers[publisher][topicname] = options;
        this.broker.subscribe(`/${publisher}/${topicname}/data`);
        this.broker.subscribe(`/${publisher}/${topicname}/schema`,{retain:true});
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
        const data = await this.preparePublishData(topicname,pub.args);
        this.broker.publish(
            `${subscriber}/${this.name}/${topicname}/data`,
            pub.encoder(data),
            pub.options
        );
        //console.log(`data : ${subscriber}/${this.name}/${topicname}/data : ${JSON.stringify(data)}`);
    }
    repubSchemas() {
        for (const topicname of Object.keys(this.publishers))
            this.publishSchema(topicname);
    }
    publishSchema(topicname,subscriber="") {
        const pub = this.publishers[topicname];
        if (!pub) return;
        this.broker.publish(
            `${subscriber}/${this.name}/${topicname}/schema`,
            pub.schema,
            {retain: subscriber.length==0, qos: 1}
        );
        console.log(`schema : ${subscriber}/${this.name}/${topicname}/schema : ${pub.schema}`);
    }

    async runInterval(interval, rate) {}

    async preparePublishData(topicname, args={}) { return {}; }
    async callbackSubscriber(publisher, topicname, data, args={}) { }

    async _setInterval(interval, rate) {
        let next = Date.now();
        while (interval._running) {
            try { await this.runInterval(interval, rate); }
            catch (err) { console.error(`Error in runInterval(${rate})`, err); }
            next += rate;
            const delay = Math.max(0, next - Date.now());
            // reuse the same sleep function every loop
            if (delay > 0)
                await this._sleep(delay);
        }
    }
    _sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
}

module.exports = TGO;
