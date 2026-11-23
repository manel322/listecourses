/*
 * server.js — point de composition serveur. Seul fichier autorisé à lire l'environnement.
 *   npm install && node server.js      → http://localhost:3000   (PORT=…, VERBOSE=1)
 */
import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";

import Broker from "./fw/broker.js";
import WebSocketChannel from "./fw/channels/websocket_channel.js";
import CATALOG from "./catalogs/catalog.js";
import { CHANNEL } from "./public/catalogs/app.catalog.js";
import EchoBoard from "./components/echo_board.js";

const PORT = Number(process.env.PORT ?? 3000);
const VERBOSE = process.env.VERBOSE === "1";
const PUBLIC = fileURLToPath(new URL("./public/", import.meta.url));
const TYPES = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8" };

const http = createServer(async (req, res) => {
	try {
		let fichier = normalize(join(PUBLIC, decodeURIComponent(new URL(req.url, "http://x").pathname)));
		if (!fichier.startsWith(PUBLIC)) { res.writeHead(403).end(); return; }
		if ((await stat(fichier).catch(() => null))?.isDirectory()) fichier = join(fichier, "index.html");
		const contenu = await readFile(fichier);
		res.writeHead(200, { "Content-Type": TYPES[extname(fichier)] ?? "application/octet-stream" });
		res.end(contenu);
	} catch { res.writeHead(404).end("introuvable"); }
});

// --- composition fwtozza ---
const broker = new Broker(CATALOG, { verbose: VERBOSE });
const wss = new WebSocketServer({ server: http });
const canal = new WebSocketChannel(broker, "canal_app", { wss, channel: CHANNEL });
const composants = [
	new EchoBoard(broker, "echo"),
];

for (const a of broker.audit([CHANNEL])) {
	(a.severity === "error" ? console.error : console.warn)(`${a.severity.toUpperCase()} (audit): ${a.message}`);
}

http.listen(PORT, () => console.log("INFO (app): http://localhost:" + PORT + " — canal " + canal.instanceName + " prêt"));

function arreter() {
	for (const c of composants) c.destroy();
	wss.close();
	http.close(() => process.exit(0));
	setTimeout(() => process.exit(0), 2000).unref();
}
process.on("SIGINT", arreter);
process.on("SIGTERM", arreter);
