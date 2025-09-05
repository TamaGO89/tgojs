const xlsx = require("xlsx");
const path = require("path");
const { readFileSync, writeFileSync, existsSync, mkdirSync } = require("fs");
const protocol_buffers_schema = require("protocol-buffers-schema");
const protocol_buffers_generator = require("pbjs/generate");


function _try_json(data) {
    if (typeof data !== "string") return data;
    data = data.trim();
    if (data.length === 0) return undefined;
    try { return JSON.parse(data); } catch (e) { return data; }
}

function _row_to_object(attributes,row) {
    const result = attributes.reduce((res,attr,i) => {
            const tempval = _try_json(row[i]);
            if (tempval !== undefined) res[attr] = tempval;
            return res;
        }, {});
    if (Object.keys(result).length>0) return result;
    else return undefined;
}

function sheet_to_object(sheet, key="_key") {
    const templist = xlsx.utils.sheet_to_json(sheet, {header:1,defval:undefined});
    if (templist.length < 1) return;
    const attributes = templist.shift();
    switch (attributes.length) {
        case 0:
            return;
        case 1:
            return templist.reduce((res,temprow) => {
                const tempval = _try_json(temprow[0]);
                if (tempval) res.push(tempval);
                return res;
            }, []);
        default:
            if (attributes[0] !== key)
                return templist.reduce((res,temprow) => {
                    const tempval = _row_to_object(attributes,temprow);
                    if (tempval) res.push(tempval);
                    return res;
                }, []);
            attributes.shift();
            if (attributes.length === 1)
                return templist.reduce((res,temprow) => {
                    const tempval = _try_json(temprow[1]);
                    if (tempval) res[temprow[0]] = tempval;
                    return res;
                }, {});
            return templist.reduce((res,temprow) => {
                    const keyname = temprow.shift();
                    const tempval = _row_to_object(attributes,temprow);
                    if (tempval) res[keyname] = tempval;
                    return res;
                }, {});
    }
}

function load_worksheet(result={},...filename) {
    const filename_fullpath = path.join(...filename);
    if (!existsSync(filename_fullpath)) return result;
    const worksheet = xlsx.readFileSync(filename_fullpath);
    for (const sheetname of worksheet.SheetNames) {
        let sheetpath = sheetname.split(".")
        sheetpath.reduce((r,v,i)=>{
                if (i == sheetpath.length - 1) {
                    const s = sheet_to_object(worksheet.Sheets[sheetname]);
                    if (r[v] === undefined) r[v] = s;
                    else if (Array.isArray(s) && Array.isArray(r[v]))
                        r[v] = r[v].concat(s);
                    else if (typeof r[v] === "object" && typeof s === "object")
                        Object.assign(r[v],s);
                } else {
                    if (r[v] === undefined) r[v] = {};
                    if (typeof r[v] !== "object") throw new Exception("boh");
                    return r[v];
                }
            }, result);
    }
    return result;
}

function load_json(...filename) {
    const filename_fullpath = path.join(...filename);
    if (!existsSync(filename_fullpath)) return {};
    return JSON.parse(readFileSync(filename_fullpath,"utf8"))
}
function write_json(data, ...filename) {
    const filename_fullpath = path.join(...filename);
    mkdirSync(path.dirname(filename_fullpath), {recursive: true});
    writeFileSync(path.join(...filename), JSON.stringify(data, null, 2), 'utf8');
}

function combine_sheets(generalized,specialized) {
    if (generalized === undefined) generalized = {};
    for (const key of Object.keys(specialized)) {
        if (key === "_") Object.assign(generalized, specialized[key]);
        else if (typeof generalized[key] === "object") Object.assign(generalized[key], specialized[key]);
        else generalized[key] = specialized[key];
    }
}

function _to_schema(variable) { return `${variable.type} ${variable.name}=${variable.tag};`; }

class ProtocolBuffer {
    constructor(name, variables=[]) {
        this.name = name;
        this.schema = "";
        this.buffer = undefined;
        this.variables = [];
        this.data = {};
        this.index = 1;
        for (const variable of variables)
            this.addVariable(variable);
        this.updateSchema();
    }
    updateSchema() {
        this.schema = `message ${this.name}{${this.variables.map((variable)=>_to_schema(variable)).join("")}}`;
    }
    loadSchema(schema) {
        const codecs = load_schema_codecs(schema, this.name);
        this.encoder = codecs.encoder;
        this.decoder = codecs.decoder;
    }
    addVariable(variable) {
        let index = this.variables.findIndex((v) => v.name === variable.name);
        if (index >= 0) return false;
        if (!variable.data) variable.data = 0;
        variable.tag = this.index++;
        this.variables.push(variable);
        this.data[variable.name] = variable.data;
        return true;
    }
    removeVariable(variable) {
        let index = this.variables.findIndex((v) => v.name === variable.name);
        if (index < 0) return false;
        this.variables.splice(index,1);
        for (let i=index; i<this.variables.length; ++i) this.variables[i].tag--;
        this.index--;
        delete this.data[variable.name];
        return true;
    }
    updateVariable(variable) {
        let index = this.variables.findIndex((v) => v.name === variable.name);
        if (index < 0 || !variable.type) return false;
        if (!variable.data) variable.data = 0;
        variable.tag = index+1;
        this.variables[index] = variable;
        this.data[variable.name] = variable.data;
        return true;
    }
}

function parse_protocol_buffer_schema(contents) {
    try {
    const result = {schema: protocol_buffers_schema.parse(contents)};
    new Function('exports', protocol_buffers_generator.generate(result.schema))(result);
    return result;
    } catch (error) {
        console.log(error);
        console.log(contents);
    }
}
function load_schema_codecs(schema,name) {
    return { encoder: schema[`encode${name}`], decoder: schema[`decode${name}`] }
}
function parse_converts(converts) {
	if (!converts) return;
	for (const conv of converts) {
        let formula = conv.formula;
        while (formula.indexOf("X") >= 0)
            formula = formula.replace("X",`remote.${conv.remote_variable}`);
        while (formula.indexOf("Y") >= 0)
            formula = formula.replace("Y",`local.${conv.local_variable}`);
		conv.cmd = new Function("remote","local",formula);
	}
}
function parse_callback(callback,args=["self","publisher","topic","message"]) {
	if (!callback) return;
	return new Function(...args,callback);
}

module.exports = {
    load_worksheet: load_worksheet,
    sheet_to_object: sheet_to_object,
    combine_sheets: combine_sheets,
    load_json: load_json,
    write_json: write_json,
    parse_protocol_buffer_schema: parse_protocol_buffer_schema,
    load_schema_codecs: load_schema_codecs,
    parse_converts: parse_converts,
    parse_callback: parse_callback,
    ProtocolBuffer: ProtocolBuffer
}
