const TGO = require("../lib/tgo");
const { load_worksheet, load_json, write_json, combine_sheets, ProtocolBuffer, parse_protocol_buffer_schema, parse_callback } = require("../lib/utils");
const fs = require('fs');
const xxhash = require("xxhashjs");
const path = require("path");


const LOG_HEAD=Buffer.of(0xBB,0x00,0xB1,0xE5);
const LOG_BODY=Buffer.of(0xBB,0x00,0xB1,0xE5);
const LOG_END=Buffer.of(0xBB,0x00,0xB1,0xE5);
const LOG_CODE = {
    SCHEMA: Buffer.of(0x01),
    DURATION: Buffer.of(0x02),
    SIZE: Buffer.of(0x04),
    NEW: Buffer.of(0xFF),
    NONE: Buffer.allocUnsafe(0)
};


class TGO_LOG extends TGO {
    constructor() {
        super("log", "/home/tamago/cmc_ws/xml/BookLog.xlsx");
        // update the received variables in a single list
        this.loadSubscribers();
        // Create a global json containing informations on all logged topics
        // their folder, creation date, last schema used, ...
    }

    checkSchema(topic,schema,timestamp) {
        console.log(`arrived schema for ${topic.info.publisher}/${topic.info.topicname}`);
        const hash = xxhash.h32(schema,0xABCD).toString(16);
        if (topic.info.schema.last === hash)
            return false;
        if (topic.schema)
            topic.schema.end = timestamp;
        if (topic.info.schema[hash]) {
            topic.schema = topic.info.schema[hash];
            delete topic.info.schema[hash].end;
        } else {
            topic.schema = {
                filename: `${hash}.schema`,
                start: timestamp,
                schema: hash,
                counter: 0
            };
            fs.writeFileSync(`${topic.path.schema}/${topic.schema.filename}`, schema, 'utf8');
            topic.info.schema[hash] = topic.schema;
            write_json(topic.info,topic.path.info);
        }
        topic.info.schema.last = hash;
        return true;
    }

    checkData(topic) {
        if (topic.options.maxSize > 0 && topic.logdata.size >= topic.options.maxSize)
            return LOG_CODE.SIZE;
        if (topic.options.maxDuration > 0 && (topic.timestamp-topic.logdata.start) >= topic.options.maxDuration)
            return LOG_CODE.DURATION;
        return LOG_CODE.NONE;
    }

    openLogFile(topic) {
        const filename = `${topic.timestamp}-${topic.info.schema.last}.log`;
        topic.logfile = fs.openSync(`${topic.path.data}/${filename}`,"w");
        topic.logdata = {
            filename: filename,
            start: Number(topic.timestamp),
            schema: topic.info.schema.last,
            counter: 0,
            size: 0
        };
        if (topic.options.maxCounter > 0 && topic.info.logdata.length >= topic.options.maxCounter) {
            const old_topic = topic.info.logdata.shift();
            const old_schema = topic.info.schema[old_topic.schema];
            fs.unlink(`${topic.path.data}/${old_topic.filename}`);
            if (--old_schema.counter <= 0) {
                delete topic.info.schema[old_topic.schema];
                fs.unlink(`${topic.path.schema}/${old_schema.filename}`);
            }
        }
        topic.schema.counter++;
        topic.info.logdata.push(topic.logdata);
        this.writeLogFile(topic,Buffer.from(topic.info.schema.last),LOG_HEAD);
        write_json(topic.info,topic.path.info);
    }

    closeLogFile(topic,reason) {
        if (!topic.logfile) return;
        this.writeLogFile(topic,reason,LOG_END);
        topic.logfile.end();
        topic.logdata.end = topic.timestamp;
        topic.logdata.code = reason[0];
    }

    writeLogFile(topic,data,header) {
        topic.bufferheader.writeBigUInt64LE(BigInt(topic.timestamp),0);
        topic.bufferheader.writeUInt32LE(data.length,8);
        fs.writeSync(topic.logfile, Buffer.concat([header,topic.bufferheader,data]));
        topic.logdata.size += 16 + data.length;
        topic.logdata.counter++;
    }

    prepareLoggedTopic(publisher,topicname,options) {
        if (!Object.hasOwn(this.logged_topics, publisher))
            this.logged_topics[publisher] = {};
        this.logged_topics[publisher][topicname] = {options: options};
        const topic = this.logged_topics[publisher][topicname];
        const main_path = `${this.options.log.localPath}/${publisher}/${topicname}`;
        topic.path = {main: main_path, data: main_path+"/data", schema: main_path+"/schema", info: main_path+"/info.json"};
        if (fs.existsSync(topic.path.info)) {
            topic.info = load_json(topic.path.info);
        } else {
            topic.info = {
                publisher: publisher,
                topicname: topicname,
                schema: {},
                logdata: [],
                start: Date.now()
            };
            fs.mkdirSync(topic.path.data,{recursive:true});
            fs.mkdirSync(topic.path.schema );
            write_json(topic.info,topic.path.info);
        }
        topic.skipped = 0;
        topic.timestamp = topic.info.start;
        topic.bufferheader = Buffer.allocUnsafe(12);
    }

    loadSubscribers() {
        this.logged_topics = {};
        for (const sub of this.options.log.subscribe) {
            const {publisher, topicname, options, ...logged_options} = Object.assign({}, this.options.log.default, sub);
            if (logged_options.maxDuration>0) logged_options.maxDuration *= 60000;
            if (logged_options.maxSize>0) logged_options.maxSize = logged_options.maxSize << 20;
            if (logged_options.maxFrequency>0) logged_options.minRate = 1000.0 / logged_options.maxFrequency;
            else logged_options.minRate = 0;
            if (logged_options.minSkipped) logged_options.minSkipped++;
            else logged_options.minSkipped = 1;
            if (!Object.hasOwn(this.logged_topics, publisher))
                this.logged_topics[publisher] = {};
            if (topicname === "+")
                this.logged_topics[publisher][topicname] = {options: logged_options};
            else
                this.prepareLoggedTopic(publisher,topicname,logged_options);
            this.subscribe(publisher,topicname,{options:options});
        }
    }

    _getTopic(publisher,topicname) {
        if (!Object.hasOwn(this.logged_topics,publisher)) return;
        const logged_publisher = this.logged_topics[publisher];
        if (!Object.hasOwn(logged_publisher,topicname)) {
            if (!Object.hasOwn(logged_publisher,"+")) return;
            this.prepareLoggedTopic(publisher,topicname,logged_publisher["+"].options);
        }
        return logged_publisher[topicname];
    }

    _getTopicUnsafe(publisher,topicname) {
        return this.logged_topics[publisher][topicname];
    }

    async onMessage(topicpath,message) {
        const [subscriber, publisher, topicname, datatype] = topicpath.split("/");
        const topic = this._getTopic(publisher,topicname);
        if (!topic) return;
        const timestamp = Date.now();
        if (datatype === "schema" && this.checkSchema(topic,message,timestamp)) {
            this.closeLogFile(topic,LOG_CODE.SCHEMA);
            this.openLogFile(topic);
        } else if (datatype === "data" && topic.schema) {
            if ((++topic.skipped%topic.options.minSkipped)>0) return;
            if ((timestamp-topic.timestamp)<topic.options.minRate) return;
            console.log(`logging ${topicpath}`);
            topic.timestamp = timestamp;
            const reason_code = this.checkData(topic);
            if (reason_code > 0) {
                console.log(`rotating logfile with reason ${reason_code[0]}`);
                this.closeLogFile(topic,reason_code);
                this.openLogFile(topic);
            }
            this.writeLogFile(topic, message, LOG_BODY); 
        }
    }
}

module.exports = TGO_LOG;
