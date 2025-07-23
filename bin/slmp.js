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

/*
const options = load_worksheet("/home/tamago/cmc_ws/xml/Book2.xlsx");
combine_sheets(options.slmp, load_worksheet("/home/tamago/cmc_ws/xml/Book1.xlsx"));
const variables = slmp.Utils.parseAllVariables(load_json(options.slmp.path,options.slmp.type,options.slmp.labels));
const rd = slmp.Utils.parseRules(variables,options.slmp.read,"read");
const wr = slmp.Utils.parseRules(variables,options.slmp.write,"write");

async function read(trigger) {
	for (let rule of rules[trigger].cmd) await rule.cmd({client:client},rule.args);
}
*/

class TGO_SLMP extends TGO {
    constructor() {
        super("slmp", "/home/tamago/cmc_ws/xml/Book1.xlsx");
        // update the received variables in a single list
        this.loadVariables();
        this.loadClient();
        this.loadReads();
        this.loadWrites();
    }
    connect() {
        this.client.connect();
        super.connect();
    }
    loadClient() {
        this.client = new slmp.Advanced.SLMP_Advanced(this.options.slmp);
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
                this.subscribe(publisher,topicname);
            }
        }
    }

    runInterval(rate) {
        const interval = this.intervals[rate];
        for (const topic of interval.topics)
            this.publishData(topic);
    }

    async preparePublishData(topicname, args={}) {
        const reads = this.reads[topicname];
        for (const read of reads.cmd)
            await read.cmd(this,read.args);
        console.log(topicname + " : server");
        // TODO : Add support for structures and array
        for (const variable of reads.variables)
            reads.protobuf.data[variable.meta.name] = variable.data[0];
        console.log(topicname + " : data");
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
        for (const write of writes.cmd)
            await write.cmd(this,write.args);
    }
}

module.exports = TGO_SLMP;
