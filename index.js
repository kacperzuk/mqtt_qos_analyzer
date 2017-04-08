const jsonfile = require('jsonfile')
const stats = require("stats-lite");
const commandLineArgs = require("command-line-args");
const mqtt = require("mqtt");
const stringify = require("csv-stringify");
const fs = require("fs");
const client = mqtt.connect("mqtt://127.0.0.1:1883");

const options = commandLineArgs([
    { name: "sub_qos", type: Number },
    { name: "file", type: String, multiple: false, defaultOption: true }
]);

if (!options.file || options.sub_qos === undefined) {
    console.error("You must specify --sub_qos and provide file for dump!");
    process.exit(1)
}

let devices = {};

client.on("connect", () => {
    client.subscribe("qos_testing/#", {
        qos: options.sub_qos
    });
});

client.on("message", (topic, message) => {
    const device = topic.split("/")[1];
    const i = parseInt(message.toString());
    const ts = process.hrtime();

    if(!devices[device]) {
        devices[device] = {
            last_i: i,
            last_timestamp: ts,
            messages: [ [Date.now(), i] ],
            intervals: [],
            seen: [],
            duplicates: [],
            out_of_order: []
        }
    } else {
        let diff = process.hrtime(devices[device].last_timestamp);
        diff = diff[0]*1000 + diff[1]/1000000.0;
        devices[device].intervals.push(diff);
        if (devices[device].seen.indexOf(i) > -1) {
            devices[device].duplicates.push([Date.now(), i]);
        } else if (devices[device].last_i >= i) {
            devices[device].out_of_order.push([Date.now(), i]);
        }
        devices[device].last_i = i;
        devices[device].seen.push(i);
        devices[device].last_timestamp = ts;
        devices[device].messages.push([Date.now(), i]);
    }
});

setInterval(function() {
    Object.keys(devices).forEach((device) => {
        let d = devices[device];
        let last_1k = d.intervals.slice(Math.max(0, d.intervals.length - 1000));
        console.log("=====");
        console.log("Status for device #" + device + " (stats for last 1k msgs)");
        console.log("Msgs rcvd:", d.messages.length);
        console.log("i:", d.last_i);
        console.log("Mean interval:", stats.mean(last_1k));
        console.log("stddev interval:", stats.stdev(last_1k));
        console.log("95th percentile interval:", stats.percentile(last_1k, 0.95));
        console.log("max interval:", Math.max(...last_1k));
        console.log("number of duplicates:", d.duplicates.length);
        console.log("number of out_of_order:", d.out_of_order.length);
    });
}, 2000);

function dump(reason, add) {
    console.log("Dumping state to " + options.file + ".json and " + options.file + "_intervals.csv");
    jsonfile.writeFileSync(options.file + ".json", devices);
    stringify([].concat.apply([], Object.keys(devices).map((device) => {
        return devices[device].intervals.map((int) => {
            return [device, int];
        });
    })), (err, out) => {
        console.log(err);
        fs.writeFileSync(options.file + "_intervals.csv", out);
        console.log("Try running ");

        if(reason) {
            console.log("Quiting because:", reason);
            if(reason == "EXCEPTION") {
                console.log(add);
            }
            process.exit();
        }
    });
}

process.on("SIGINT", dump.bind(null, "SIGINT"));
process.on("SIGHUP", dump.bind(null, false));
process.on("uncaughtException", dump.bind(null, "EXCEPTION"));
