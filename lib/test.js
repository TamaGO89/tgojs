const broker = require("mqtt");
const { load_worksheet, combine_sheets } = require("/home/tamago/cmc_ws/tgojs/lib/utils");
const name = "test1";
const options = load_worksheet("/home/tamago/cmc_ws/xml/Book2.xlsx");
options.mqtt.clientId = name;
if (options.mqtt.options.will) {
    options.mqtt.options.will = {
        topic: `/${options.mqtt.clientId}/keepalive`,
        payload: Buffer.of(0),
        qos: 1,
        retain: true
    };
}

cli = broker.connect(`${options.mqtt.protocol}://${options.mqtt.address}:${options.mqtt.port}`,options.mqtt.options);
cli.on("connect",(u,d)=>{console.log(u);console.log(d);});
cli.subscribe("/test1/#");
cli.subscribe("/test2/#");
cli.on("message",(t,m)=>{console.log(t);console.log(m.toString());});




