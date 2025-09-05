const TGO = require("../lib/tgo");
const { load_worksheet, load_json, write_json, combine_sheets, ProtocolBuffer, load_schema_codecs, parse_protocol_buffer_schema, parse_callback } = require("../lib/utils");
const fs = require('fs');
const xxhash = require("xxhashjs");
const path = require("path");


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


class TGO_TEST extends TGO {
    constructor() { super("test"); }

    onConnect() {
        super.onConnect();
        console.log("connected");
    }

    async callbackSubscriber(publisher, topicname, data, args={}) {
        console.log(`/${publisher}/${topicname} : ${JSON.stringify(data)}`);
    }
}

module.exports = TGO_TEST;
