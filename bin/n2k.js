const TGO = require("../lib/tgo");
const { load_worksheet, load_json, combine_sheets, ProtocolBuffer, parse_protocol_buffer_schema, parse_callback } = require("../lib/utils");
const n2k = require("/home/tamago/cmc_ws/cmc-canboatjs/index");
const { getPgnById, getPgn0 } = require("/home/tamago/cmc_ws/cmc-canboatjs/lib/pgns");


function N2K_TO_MQTT({Id, BitLength, Resolution, Signed, FieldType}) {
    switch(FieldType) {
        case "LOOKUP":
        case "BITLOOKUP":
            return {name: Id, type: (BitLength < 2 ? "bool" : BitLength < 32 ? "uint32" : "uint64"), data: 0};
        case "INDIRECT_LOOKUP":
        case "FIELDTYPE_LOOKUP":
            return {name: Id, type: (BitLength < 32 ? "uint32" : "uint64"), data: 0};
        case "DATE":
        case "TIME":
        case "DURATION":
            return {name: Id, type: (BitLength < 32 ? "uint32" : "uint64"), data: 0};
        case "MMSI":
        case "ISO_NAME":
        case "PGN":
            return {name: Id, type: (BitLength < 32 ? "uint32" : "uint64"), data: 0};
        case "FIELD_INDEX":
        case "DYNAMIC_FIELD_KEY":
        case "DYNAMIC_FIELD_LENGTH":
            return {name: Id, type: (BitLength < 32 ? "uint32" : "uint64"), data: 0};
        case "NUMBER":
        case "DECIMAL":
            if (Resolution < 1) return {name: Id, type: "float", data: 0.0};
            return {name: Id, type: ((Signed ? "s":"u") + "int" + (BitLength<32 ? "32":"64")), data: 0};
        case "FLOAT":
            return {name: Id, type: "float", data: 0.0};
        case "RESERVED":
        case "SPARE":
            return {name: Id, type: "bool", data: false};
        case "DYNAMIC_FIELD_VALUE":
        case "BINARY":
        case "VARIABLE":
        case "KEY_VALUE":
            return {name: Id, type: "bytes", data: ""};
        case "STRING_LZ":
        case "STRING_FIX":
        case "STRING_LAU":
            return {name: Id, type: "string", data: ""};
    }
}


class TGO_N2K extends TGO {
    constructor() {
        super("n2k", "/home/tamago/cmc_ws/xml/BookN2K.xlsx");
        // update the received variables in a single list
        if (!this.options.n2k.receiveIDs) this.options.n2k.receiveIDs = [];
        if (!this.options.n2k.receivePGNs) this.options.n2k.receivePGNs = [];
        if (!this.options.n2k.transmitIDs) this.options.n2k.transmitIDs = [];
        if (!this.options.n2k.transmitPGNs) this.options.n2k.transmitPGNs = [];
        for (const source of this.options.n2k.sources) {
            const fields = [];
            for (const [key,value] of Object.entries(source)) {
                if (key==="source") continue;
                fields.push([key,value]);
                delete source[key];
            }
            source.fields = fields;
        }
        this.loadReceives();
        this.loadTransmits();
        this.loadClient();
    }

    loadClient() {
        this.client = new n2k.N2K_Client(this.options.n2k);
        this.client.on("data",(data)=>{this.onData(data);});
    }

    onConnect() {
        this.client.connect();
        super.onConnect();
    }

    async runInterval(interval, rate) {
        for (const pgn of interval.pgns)
            this.client.send(this.trns_pgns[pgn]);
        for (const topicname of interval.topics)
            this.publishData(topicname);
    }

    loadReceives() {
        this.pub_topics = {};
        this.recv_pgns = {};
        for (const receive of this.options.n2k.receive) {
            if (typeof receive.pgn === "number")
                receive.pgn = getPgn0(receive.pgn).Id; 
            const pgn_info = getPgnById(receive.pgn);
            if (this.options.n2k.receiveIDs.indexOf(pgn_info.Id) < 0)
                this.options.n2k.receiveIDs.push(pgn_info.Id);
            if (this.options.n2k.receivePGNs.indexOf(pgn_info.PGN) < 0)
                this.options.n2k.receivePGNs.push(pgn_info.PGN);
            if (!Object.hasOwn(this.pub_topics,receive.topicname))
                this.pub_topics[receive.topicname] = {protobuf: new ProtocolBuffer(receive.topicname)};
            const topic = this.pub_topics[receive.topicname];
            for (const field of pgn_info.Fields) 
                topic.protobuf.addVariable(N2K_TO_MQTT(field));
            if (!Object.hasOwn(this.recv_pgns,receive.pgn))
                this.recv_pgns[receive.pgn] = {};
            if (!Object.hasOwn(this.recv_pgns[receive.pgn],receive.source))
                this.recv_pgns[receive.pgn][receive.source] = {
                    topics: [],
                    data: {},
                    callback: parse_callback(receive.callback,["self","source","data"])
                };
            const source_pgn = this.recv_pgns[receive.pgn][receive.source];
            if (source_pgn.topics.findIndex((topic)=>(topic.name===receive.topicname)) < 0)
                source_pgn.topics.push({name:receive.topicname,trigger:receive.trigger});
        }
        for (const advertise of this.options.n2k.advertise) {
            if (!Object.hasOwn(this.pub_topics,advertise.topicname))
                this.pub_topics[advertise.topicname] = {protobuf: new ProtocolBuffer(advertise.topicname)};
            const topic = this.pub_topics[advertise.topicname];
            topic.options = advertise.options ? advertise.options : null;
            if (!advertise.rate)
                continue;
            topic.rate = advertise.rate;
            if (!Object.hasOwn(this.intervals,topic.rate))
                this.intervals[topic.rate] = {topics: [], pgns: []};
            this.intervals[topic.rate].topics.push(advertise.topicname);
        }
        for (const topic of Object.values(this.pub_topics))
            topic.protobuf.updateSchema();
        this.reads_schema = parse_protocol_buffer_schema(
            Object.values(this.pub_topics).map((t)=>(t.protobuf.schema)).join("")
        );
        for (const topic of Object.values(this.pub_topics)) {
            topic.protobuf.loadSchema(this.reads_schema);
            this.advertise(topic.protobuf.name,topic.protobuf.schema,topic.protobuf.encoder,{options:topic.options});
        }
    }

    loadTransmits() {
        this.trns_pgns = {};
        this.sub_topics = {};
        for (const conv of this.options.n2k.convert) {
            if (typeof conv.pgn === "number")
                conv.pgn = getPgn0(conv.pgn).Id;
            const pgn_info = getPgnById(conv.pgn);
            if (this.options.n2k.transmitIDs.indexOf(pgn_info.Id) < 0)
                this.options.n2k.transmitIDs.push(pgn_info.Id);
            if (this.options.n2k.transmitPGNs.indexOf(pgn_info.PGN) < 0)
                this.options.n2k.transmitPGNs.push(pgn_info.PGN);
        }
        for (const sub of this.options.n2k.subscribe) {
            for (const i in sub.pgns) {
                if (typeof sub.pgns[i] === "number")
                    sub.pgns[i] = getPgn0(sub.pgns[i]).Id;
                const pgn_info = getPgnById(sub.pgns[i]);
                if (this.options.n2k.transmitIDs.indexOf(pgn_info.Id) < 0)
                    this.options.n2k.transmitIDs.push(pgn_info.Id);
                if (this.options.n2k.transmitPGNs.indexOf(pgn_info.PGN) < 0)
                    this.options.n2k.transmitPGNs.push(pgn_info.PGN);
            }
            if (!Object.hasOwn(this.sub_topics,sub.publisher))
                this.sub_topics[sub.publisher] = {};
            this.sub_topics[sub.publisher][sub.topicname] = {
                pgns: sub.pgns,
                callback: parse_callback(sub.callback),
                converts: this.options.n2k.convert.filter(
                    (conv)=>(conv.topicname===sub.topicname && conv.publisher===sub.publisher))
            };
        }
        for (const pgn of this.options.n2k.transmitIDs) {
            const pgn_info = getPgnById(pgn);
            this.trns_pgns[pgn_info.Id] = {pgn: pgn_info.PGN};
            pgn_info.Fields.reduce((result,{Id})=>{result[Id]=undefined;return result;},this.trns_pgns[pgn_info.Id]);
            let options = this.options.n2k.transmit.find((t)=>t.pgn===pgn_info.Id) || {priority:0,destination:255,rate:0};
            Object.assign(this.trns_pgns[pgn_info.Id],{dst: options.destination, prio: options.priority, rate: options.rate});
            if (!options.rate)
                continue;
            if (!Object.hasOwn(this.intervals,options.rate))
                this.intervals[options.rate] = {topics: [], pgns: []};
            this.intervals[options.rate].pgns.push(pgn_info.Id);
        }
        for (const sub of this.options.n2k.subscribe)
            this.subscribe(sub.publisher,sub.topicname,{options:sub.options});
    }

    onData(data) {
        const pgn = this.recv_pgns[data.Id];
        if (!pgn) return;
        for (const source of this.client.getSources(data.src,this.options.n2k.sources)) {
            const pgn_source = pgn[source];
            if (!pgn_source) continue;
            if (pgn_source.callback)
                pgn_source.callback(this,source,data);
            pgn_source.data = data.fields;
            for (const topic of pgn_source.topics) {
                Object.assign(this.pub_topics[topic.name].protobuf.data,data.fields);
                if (topic.trigger)
                    this.publishData(topic.name);
            }
        }
    }

    async preparePublishData(topicname, args={}) {
        return this.pub_topics[topicname].protobuf.data;
    }

    async callbackSubscriber(publisher, topicname, data, args={}) {
        const sub = this.sub_topics[publisher][topicname];
        if (sub.callback)
            await sub.callback(this,publisher,topicname,data);
        for (const conv of sub.converts)
            conv.cmd(data,this.trns_pgns[conv.pgn]);
        for (const pgn of sub.pgns)
            this.client.send(this.trns_pgns[pgn]);
    }
}

module.exports = TGO_N2K;

test = new TGO_N2K();
test.connect();
test.start();
