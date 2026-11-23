// tests/test_echo_board.js — node tests/test_echo_board.js
import Broker from "../fw/broker.js";
import CATALOG from "../catalogs/catalog.js";
import EchoBoard from "../components/echo_board.js";
import { Probe } from "./probe.js";

const broker = new Broker(CATALOG);
const probe = new Probe("probe");
broker.register(probe, [{ name: "ping", version: 1 }], [{ name: "pong", version: 1 }]);
new EchoBoard(broker, "echo");

broker.publish(probe, { ping: { texte: "bonjour" }, id: "s1", session: { data: {} } });
const ok = probe.received[0]?.pong?.texte === "BONJOUR" && probe.received[0]?.id === "s1";
console.log(ok ? "test_echo_board : tout passe" : "test_echo_board : ÉCHEC");
process.exit(ok ? 0 : 1);
