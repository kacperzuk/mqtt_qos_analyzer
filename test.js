const commandLineArgs = require("command-line-args");
const mqtt = require("mqtt");
const client = mqtt.connect("mqtt://127.0.0.1:1883");

const options = commandLineArgs([{ name: "pub_qos", defaultOption: 0, type: Number }]);

let i = 0;

function spam() {
    client.publish("qos_testing/7", (i++).toString(), { qos: options.pub_qos });
}

client.on("connect", function() {
    spam()
    setInterval(spam, 5);
});
