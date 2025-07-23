const TGO = require("../lib/tgo");
const { load_worksheet, load_json, combine_sheets, ProtocolBuffer, parse_protocol_buffer_schema, parse_callback } = require("../lib/utils");


class TGO_N2K extends TGO {
    constructor() {
        super("log", "/home/tamago/cmc_ws/xml/BookLog.xlsx");
        // update the received variables in a single list
        this.loadReceives();
        this.loadTransmits();
        this.loadClient();
    }

    loadClient() {
        this.client = new n2k.N2K_Client(this.options.n2k);
        this.client.on("data",(data)=>{this.onData(data);});
    }

    connect() {
        this.client.connect();
        super.connect();
    }

    runInterval(rate) {
        const interval = this.intervals[rate];
        for (const pgn of interval.pgns)
            this.client.send(this.trns_pgns[pgn]);
        for (const topicname of interval.topics)
            this.publishData(topicname)
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
            this.advertise(topic.protobuf.name,{
                cmd: (self,tpc)=>(self.pub_topics[tpc].protobuf.data),
                args: undefined,
                schema: topic.protobuf.schema,
                options: topic.options,
                encoder: topic.protobuf.encoder
            });
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
            this.subscribe(sub.publisher,sub.topicname,{
                cmd: async function(self,pub,tpc,data) {
                    const sub = self.sub_topics[pub][tpc];
                    if (sub.callback)
                        await sub.callback(self,pub,tpc,data);
                    for (const conv of sub.converts)
                        conv.cmd(data,self.trns_pgns[conv.pgn]);
                    for (const pgn of sub.pgns)
                        self.client.send(self.trns_pgns[pgn]);
                },
                args: undefined
            });
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
}

module.exports = TGO_N2K;
