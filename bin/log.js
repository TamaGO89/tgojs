const TGO = require("../lib/tgo");
const { load_json, write_json, load_schema_codecs, parse_protocol_buffer_schema } = require("../lib/utils");
const fs = require('fs');
const xxhash = require("xxhashjs");
const path = require("path");
const { parse_args, get_xlsx_config } = require("../lib/exec");
const sys_argv = parse_args(process.argv.slice(2));
if (!sys_argv.node_name) sys_argv.node_name = "log";
const xlsx_config = get_xlsx_config(sys_argv,process.env);


const LOG_HEAD=Buffer.of(0x0B,0x00,0xB1,0xE5);
const LOG_BODY=Buffer.of(0x1B,0x00,0xB1,0xE5);
const LOG_END=Buffer.of(0x2B,0x00,0xB1,0xE5);
const LOG_CODE = {
    SCHEMA: Buffer.of(0x01),
    DURATION: Buffer.of(0x02),
    SIZE: Buffer.of(0x04),
    NEW: Buffer.of(0xFF),
    NONE: Buffer.allocUnsafe(0)
};


class TGO_LOG extends TGO {
    constructor() {
        super(sys_argv, xlsx_config);
        // update the received variables in a single list
        this.loadSubscribers();
        // Create a global p containing informations on all logged topics
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
            topic.info_timestamp = topic.timestamp;
        }
        topic.info.schema.last = hash;
        return true;
    }

    checkData(topic) {
        if (!topic.logfile) return;
        if (topic.options.maxSize > 0 && topic.logdata.size >= topic.options.maxSize)
            return LOG_CODE.SIZE;
        if (topic.options.maxDuration > 0 && (topic.timestamp-topic.logdata.start) >= topic.options.maxDuration)
            return LOG_CODE.DURATION;
        if (topic.timestamp - topic.info_timestamp > 600000) {
            write_json(topic.info,topic.path.info);
            topic.info_timestamp = topic.timestamp;
        }
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
            try {
                const old_topic = topic.info.logdata.shift();
                const old_schema = topic.info.schema[old_topic.schema];
                fs.unlinkSync(`${topic.path.data}/${old_topic.filename}`);
                if (--old_schema.counter <= 0) {
                    delete topic.info.schema[old_topic.schema];
                    fs.unlinkSync(`${topic.path.schema}/${old_schema.filename}`);
                }
            } catch (e) { console.error(e); }
        }
        topic.schema.counter++;
        topic.info.logdata.push(topic.logdata);
        this.writeLogFile(topic,Buffer.from(topic.info.schema.last),LOG_HEAD);
        write_json(topic.info,topic.path.info);
        topic.info_timestamp = topic.timestamp;
    }

    closeLogFile(topic,reason) {
        if (!topic.logfile) return;
        this.writeLogFile(topic,reason,LOG_END);
        fs.close(topic.logfile);
        delete topic.logfile;
        topic.logdata.end = topic.timestamp;
        topic.logdata.code = reason[0];
    }

    writeLogFile(topic,data,header) {
        if (!topic.logfile) return;
        topic.bufferheader.writeBigUInt64LE(BigInt(topic.timestamp),0);
        topic.bufferheader.writeUInt32LE(data.length,8);
        fs.writeSync(topic.logfile, Buffer.concat([header,topic.bufferheader,data]));
        topic.logdata.size += 16 + data.length;
        topic.logdata.counter++;
    }

    prepareLoggedTopic(publisher,topicname,options) {
        if (!this.logged_topics[publisher])
            this.logged_topics[publisher] = {};
        this.logged_topics[publisher][topicname] = {options: options};
        const topic = this.logged_topics[publisher][topicname];
        const main_path = `${this.options.log.localPath}/${publisher}/${topicname}`;
        topic.path = {main: main_path, data: main_path+"/data", schema: main_path+"/schema", info: main_path+"/info.json"};
        if (fs.existsSync(topic.path.info)) {
            topic.info = load_json(topic.path.info);
            delete topic.info.schema.last;
        } else {
            topic.info = {
                publisher: publisher,
                topicname: topicname,
                schema: {},
                logdata: [],
                start: Date.now()
            };
            fs.mkdirSync(topic.path.data,{recursive:true});
            fs.mkdirSync(topic.path.schema,{recursive:true} );
            write_json(topic.info,topic.path.info);
        }
        topic.skipped = 0;
        topic.timestamp = topic.info.start;
        topic.info_timestamp = topic.timestamp;
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
            if (!this.logged_topics[publisher])
                this.logged_topics[publisher] = {};
            if (topicname === "+")
                this.logged_topics[publisher][topicname] = {options: logged_options};
            else
                this.prepareLoggedTopic(publisher,topicname,logged_options);
            this.subscribe(publisher,topicname,{options:options});
        }
    }

    _getTopic(publisher,topicname) {
        if (!this.logged_topics[publisher]) return;
        const logged_publisher = this.logged_topics[publisher];
        if (!logged_publisher[topicname]) {
            if (!logged_publisher["+"]) return;
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
            topic.skipped = (topic.skipped+1) % topic.options.minSkipped;
            if (topic.skipped>0) return;
            if ((timestamp-topic.timestamp)<topic.options.minRate) return;
            //console.log(`logging ${topicpath}`);
            topic.timestamp = timestamp;
            // TODO : Can't do this at each loop, just put this in an interval and check every minute or two
            // any closeLogFile operation should reset this reason_code
            const reason_code = this.checkData(topic);
            if (reason_code.length > 0) {
                console.log(`rotating logfile with reason ${reason_code[0]}`);
                this.closeLogFile(topic,reason_code);
                this.openLogFile(topic);
            }
            this.writeLogFile(topic, message, LOG_BODY); 
        }
    }
}


function _parse(buffer,offset) {
    const timestamp = Number(buffer.readBigUInt64LE(offset+4));
    const size = buffer.readUInt32LE(offset+12);
    return {
        timestamp: timestamp, size: size,
        data: buffer.slice(offset+16,offset+16+size)
    }
}

function parse(logpath,publisher,topicname) {
    const logfullpath = `${logpath}/${publisher}/${topicname}`;
    const log = {
        info: JSON.parse(fs.readFileSync(logfullpath+"/info.json")),
        schema: {},
        logdata: []
    };
    console.log("info loaded:");
    console.log(log.info);
    for (const [key,val] of Object.entries(log.info.schema)) {
        if (key==="last") continue;
        log.schema[key] = {
            source: fs.readFileSync(logfullpath+"/schema/"+val.filename).toString()
        };
        log.schema[key].decoder = load_schema_codecs(parse_protocol_buffer_schema(log.schema[key].source),topicname).decoder;
    }
    log.schema.last = log.schema[log.info.schema.last];
    console.log("schema loaded:");
    console.log(Object.keys(log.info.schema));
    for (const val of log.info.logdata) {
        const logdata = {source: fs.readFileSync(logfullpath+"/data/"+val.filename), data: []};
        let offset = 0;
        let error = false;
        while (offset + 16 < logdata.source.length) {
            const header = logdata.source.slice(offset,offset+4);
            if (header.equals(LOG_HEAD)) {
                error = false;
                payload = _parse(logdata.source,offset);
                logdata.schema = log.schema[payload.data.toString()];
                offset += 16+payload.size;
            } else if (header.equals(LOG_BODY)) {
                if (error) {
                    console.log("recovered from header error");
                    console.log(offset);
                    console.log(logdata.source.slice(Math.max(0,offset-20),offset+60));
                    error = false;
                }
                payload = _parse(logdata.source,offset);
                try {
                    payload.data = logdata.schema.decoder(payload.data);
                    logdata.data.push(payload);
                } catch (e) {
                    console.log(e);
                    console.log(offset);
                    console.log(payload.size);
                    console.log(logdata.source.slice(offset,offset+16+payload.size));
                }
                offset += 16+payload.size;
            } else if (header.equals(LOG_END)) {
                payload = _parse(logdata.source,offset);
                offset += 16+payload.size;
            } else {
                if (!error) {
                    console.log("header don't match");
                    console.log(offset);
                    console.log(logdata.source.slice(Math.max(0,offset-20),offset+60));
                    error = true;
                }
                offset++;
            }
        }
        log.logdata.push(logdata);
    }
    return log;
}

function save_parsed(source,logpath,publisher,topicname) {
    const logfullpath = `${logpath}/${publisher}/${topicname}/parsed.json`;
    const parsed = [];
    for (const logdata of source.logdata)
        parsed.push(...logdata.data);
    const sorted = parsed.sort(d=>d.timestamp);
    fs.writeFileSync(logfullpath,JSON.stringify(sorted,null,4));
}
function parse_all(logpath) {
    for (const publisher of fs.readdirSync(logpath)) {
        for (const topicname of fs.readdirSync(`${logpath}/${publisher}`)) {
            const parsed = parse (logpath,publisher,topicname);
            save_parsed(parsed,logpath,publisher,topicname);
        }
    }
}

module.exports = TGO_LOG;

test = new TGO_LOG();
test.connect();
test.start();
