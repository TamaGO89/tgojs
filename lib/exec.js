
function parse_args(argv, opts = {}) {
    const result = { _: [] };
    const { alias = {}, boolean = [], string = [] } = opts;
    string.push("specific_path","specific_name","node_name");
    const boolSet = new Set(Array.isArray(boolean) ? boolean : [boolean]);
    const strSet = new Set(Array.isArray(string) ? string : [string]);

    for (let i = 0; i < argv.length; i++) {
        const arg = argv[i];
        if (arg.startsWith('--')) {
            const [key, value] = arg.slice(2).split('=');

            if (boolSet.has(key))
                result[key] = true;
            else if (strSet.has(key))
                result[key] = value !== undefined ? value : argv[++i];
            else
                result[key] = value !== undefined ? value : true;

            for (const short in alias) {
                if (alias[short] === key) {
                    result[key] = result[key];
                    result[alias[short]] = result[key];
                }
            }

        } else if (arg.startsWith('-') && arg.length > 1) {
            const flags = arg.slice(1).split('');

            for (const f of flags) {
                const longName = alias[f] || f;

                if (boolSet.has(f) || boolSet.has(longName))
                    result[longName] = true;
                else if (strSet.has(f) || strSet.has(longName)) {
                    const val = argv[i + 1];
                    if (val && !val.startsWith('-')) {
                        result[longName] = val;
                        i++;
                    } else
                        result[longName] = '';
                } else
                    result[longName] = true;
            }

        } else
            result._.push(arg);
    }
    return result;
}

function get_xlsx_config(sys_argv,sys_env) {
    return {
        default: {
            path: sys_argv.default_path || sys_env.TGO_XLSX_DEFAULT_PATH,
            name: sys_argv.default_name || sys_env.TGO_XLSX_DEFAULT_NAME
        },
        specific: {
            path: sys_argv.specific_path || sys_env.TGO_XLSX_DEFAULT_PATH,
            name: sys_argv.specific_name || `${sys_argv.node_name}.xlsx`
        }
    };
}

module.exports = {
    parse_args: parse_args,
    get_xlsx_config: get_xlsx_config
}
