 const TGO = require("../lib/tgo");
const { load_worksheet, load_json, combine_sheets, ProtocolBuffer, parse_protocol_buffer_schema, parse_callback } = require("../lib/utils");
const slmp = require("/home/tamago/cmc_ws/cmc_melsec/index");


const SLMP_TO_PB = {
    BOOL: "bool",
    INT: "sint32",
    DINT: "sint32",
    WORD: "uint32",
    DWORD: "uint32",
    REAL: "float",
    LREAL: "double",
    string: "bytes",
    wstring: "string"
};


class TGO_SLMP extends TGO {
    constructor() {
        super("slmp", "/home/tamago/cmc_ws/xml/Book1.xlsx");
        // update the received variables in a single list
        this.loadVariables();
        this.loadReads();
        this.loadWrites();
        this.loadClient();
    }
    start() {
        for (const reads of Object.values(this.reads)) for (const read of reads.cmd)
            read.buffer = read.cmd.init(this,read.args);
        for (const publisher of Object.values(this.writes)) for (const writes of Object.values(publisher)) for (const write of writes.cmd)
            write.buffer = write.cmd.init(this,write.args);
        super.start();
    }
    loadClient() {
        this.client = new slmp.Advanced.SLMP_Advanced(this.options.slmp);
        this.client.on("ready", () => this.start());
        this.client.connect();
    }
    loadVariables() {
        this.variables = slmp.Utils.parseAllVariables(load_json(this.options.slmp.path,this.options.slmp.type,this.options.slmp.labels));
    }
    loadReads() {
        this.reads = slmp.Utils.parseRules(this.variables,this.options.slmp.read,"read");
        //var schema = "";
        for (const [name,topic] of Object.entries(this.reads)) {
            topic.protobuf = new ProtocolBuffer(name);
            for (const variable of topic.variables)
                topic.protobuf.addVariable({
                    name: variable.meta.name,
                    type: SLMP_TO_PB[variable.meta.type.source],
                    source: variable
                });
            topic.protobuf.updateSchema();
        }
        this.reads_schema = parse_protocol_buffer_schema(
            Object.values(this.reads).map((t)=>(t.protobuf.schema)).join("")
        );
        for (const [name,topic] of Object.entries(this.reads)) {
            topic.protobuf.loadSchema(this.reads_schema);
            this.advertise(topic.protobuf.name,topic.protobuf.schema,topic.protobuf.encoder,{options:topic.options});
            if (!topic.rate)
                continue;
            if (!Object.hasOwn(this.intervals,topic.rate))
                this.intervals[topic.rate] = {topics: []};
            this.intervals[topic.rate].topics.push(topic.protobuf.name);
        }
    }
    loadWrites() {
        for (const write of this.options.slmp.write) {
            write.variables = write.variables.concat( this.options.slmp.convert
                .filter((conv)=>(conv.topicname===write.topicname && conv.publisher===write.publisher && write.variables.indexOf(conv.local_variable)<0))
                .map((conv)=>(conv.local_variable)) );
        }
        const pubs = this.options.slmp.write.map((v)=>(v.publisher)).filter((v,i,a)=>(a.indexOf(v)===i));
        this.writes = {};
        for (const pub of pubs) {
            const filtered_writes = this.options.slmp.write.filter((v)=>(v.publisher===pub));
            this.writes[pub] = slmp.Utils.parseRules( this.variables, filtered_writes, "write" );
        }
        for (const [publisher,group] of Object.entries(this.writes)) {
            for (const [topicname,topic] of Object.entries(group)) {
                topic.data = topic.variables.reduce((result,variable)=>{result[variable.meta.name]=undefined;return result;},{});
                topic.converts = this.options.slmp.convert.filter((conv)=>(conv.topicname===topicname && conv.publisher===publisher));
                topic.callback = parse_callback(
                    this.options.slmp.write.find((conv)=>(conv.topicname===topicname && conv.publisher===publisher)).callback,
                    ["self","publisher","topic","message"] );
                this.subscribe(publisher,topicname,{options:topic.options});
            }
        }
    }

    // TODO : Fix this interval to work without "setInterval", cause it sucks hard
    async runInterval(interval, rate) {
        await new Promise((resolve) => {
            let waiting = interval.topics.length;
            if (waiting<1) return resolve();
            for (const topic of interval.topics)
                this.publishData(topic).finally(()=>{if(--waiting<1)resolve();});
        });
        //for (const topic of interval.topics) await this.publishData(topic);
    }

    async preparePublishData(topicname, args={}) {
        const reads = this.reads[topicname];
        await new Promise((resolve) => {
            let waiting = reads.cmd.length;
            if (waiting<1) return resolve();
            for (const read of reads.cmd)
                this.client.sendRead(read.buffer).then(
                        // TODO : Put a try catch here, if "recv" fails i may fuck this up
                        response => read.cmd.recv(this,read.buffer,response,read.args),
                        // TODO : Do some error management here, like this is pointless
                        error => console.log(error)).finally(()=>{if(--waiting<1)resolve();});
        });
        //console.log(topicname + " : server");
        // TODO : Add support for structures and array
        for (const variable of reads.variables)
            reads.protobuf.data[variable.meta.name] = variable.data[0];
        //console.log(topicname + " : data");
        return reads.protobuf.data;
    }

    async callbackSubscriber(publisher, topicname, data, args={}) {
        const writes = this.writes[publisher][topicname];
        if (writes.callback)
            writes.callback(this,publisher,topicname,data);
        for (const conv of writes.converts)
            conv.cmd(data,writes.data);
        for (const variable of writes.variables)
            variable.data[0] = writes.data[variable.meta.name];
        await new Promise((resolve) => {
            let waiting = writes.cmd.length;
            if (waiting<1) return resolve();
            for (const write of writes.cmd)
                write.cmd.send(this,write.buffer,write.args).then(
                        // TODO : Put a try catch here, if "recv" fails i may fuck this up
                        response => this.client.recvWrite(write.buffer,response),
                        // TODO : Do some error management here, like this is pointless
                        error => console.log(error)).finally(()=>{if(--waiting<1)resolve();});
        });
    }
}

module.exports = TGO_SLMP;

test = new TGO_SLMP();
