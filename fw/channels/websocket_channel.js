/*
 * fw/channels/websocket_channel.js — canal WebSocket, moitié serveur.
 *
 * Un canal est un objet standard : (broker, instanceName, deps). Il est le seul
 * objet qui connaisse le transport ; les composants, eux, ne voient que le broker.
 *
 *   deps.wss                 le WebSocketServer déjà ouvert — le point de composition
 *                            l'a créé, c'est lui qui le fermera (règles 4 et 5 de deps)
 *   deps.channel             le CHANNEL du catalogue de canal : clientEmits / clientReceives
 *   deps.sessionTtlMs        survie d'une session déconnectée (défaut 5 min)
 *   deps.handshakeTimeoutMs  délai accordé au client pour se présenter (défaut 10 s)
 *   deps.maxFrameBytes       taille maximale d'une trame (défaut 256 Kio)
 *
 * Inscription en miroir croisé de CHANNEL : le canal émet localement ce que les
 * clients émettent, et reçoit localement ce que les clients reçoivent — plus
 * connection_event, le contrat du framework qui annonce l'état des sessions.
 *
 * Protocole :
 *   1. handshake_request  (client → serveur) : millésime, listes de transit et
 *      schémas complets des messages qui traversent, jeton de session éventuel.
 *   2. handshake_response (serveur → client) : jeton de session et « baselines »
 *      (la révision que le serveur émet pour chaque message serveur→client), ou
 *      une erreur si les catalogues sont incompatibles — le socket est alors fermé.
 *   3. Ensuite, chaque trame est une enveloppe fwtozza : { nom_du_message: payload }.
 *
 * Le handshake juge la COMPATIBILITÉ des catalogues, pas leur identité : mêmes
 * messages et versions, schémas identiques sur les révisions partagées, et règle
 * directionnelle « le récepteur est au moins aussi à jour que l'émetteur ». Un
 * client en retard d'une révision passe : le canal upcaste ses trames jusqu'au
 * latest du serveur avant de les publier.
 *
 * Sessions : le handshake rend un jeton ; un client qui le représente reprend
 * sa session, qui survit sessionTtlMs à une coupure. Tout message entrant est
 * publié avec les clés d'enveloppe du framework : "id" (identifiant de session,
 * stable à travers les reconnexions) et "session" (l'objet { data } partagé, joint
 * par référence : ce qu'un composant y écrit, les suivants le lisent). Aucune
 * des deux ne part jamais sur le réseau.
 *
 * Admission : avant de publier un entrant, le canal consulte session.allowMessage
 * si un composant d'authentification l'a posée. Absente, tout passe (il faut bien
 * que le login arrive). La validation du payload, elle, est l'affaire du broker,
 * à la publication.
 *
 * Pas de destroy() : un canal ne se démonte pas.
 */

import { randomUUID } from "crypto";
import { WebSocket } from "ws";
import { latestRevision, upcastToLatestRevision, canonicalStringify } from "../engine.js";

const CONNECTION_EVENT = { name: "connection_event", version: 1 };

class WebSocketChannel {

	instanceName;
	broker;
	deps;
	verbose = false;

	emits;
	receives;

	#clientEmits;      // client → serveur : [{ name, version }]
	#clientReceives;   // serveur → client : [{ name, version }]
	#emitVersion = new Map(); // nom → version, pour les messages client → serveur
	#analysisCache = new Map(); // contrat client (forme canonique) → analyse, partagée entre clients identiques

	// identifiant de session → { session: { data }, socket | null, expiryTimer, emitFrom }
	// emitFrom : nom → révision que CE client émet, apprise au handshake
	#sessions = new Map();

	constructor(broker, instanceName, deps = {}) {
		this.broker = broker;
		this.instanceName = instanceName;
		this.deps = deps;
		this.#clientEmits = deps.channel.clientEmits;
		this.#clientReceives = deps.channel.clientReceives;
		for (const { name, version } of this.#clientEmits) this.#emitVersion.set(name, version);

		// miroir croisé : ce que le client émet, le canal l'émet ici ; ce que le
		// client reçoit, le canal le reçoit ici pour l'envoyer. Sans révision :
		// seules des formes canoniques circulent entre les deux brokers.
		this.emits = [...this.#clientEmits, CONNECTION_EVENT];
		this.receives = [...this.#clientReceives];
		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): register");
		if (!this.broker.register(this, this.emits, this.receives)) {
			// inscription refusée en bloc (un message du CHANNEL manque au catalogue ?) :
			// le canal reste inactif, l'application continue sans lui
			console.error("ERROR (" + this.instanceName + "): inscription rejetée par le broker — canal inactif");
			return;
		}
		deps.wss.on("connection", (socket) => this.#accept(socket));
	}

	// Sortant : une émission d'un objet serveur à destination des clients.
	// Sans "id" : diffusion à toutes les sessions connectées ; avec : seulement
	// celles-là. Les clés d'enveloppe réservées sont retirées avant l'envoi.
	handleMessage(message) {
		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): reçoit " + JSON.stringify(message));
		const outgoing = { ...message };
		const targets = outgoing.id === undefined ? null : [outgoing.id].flat();
		delete outgoing.id;
		delete outgoing.session;
		const frame = JSON.stringify(outgoing);
		for (const [sessionId, entry] of this.#sessions) {
			if (targets !== null && !targets.includes(sessionId)) continue;
			if (entry.socket === null || entry.socket.readyState !== WebSocket.OPEN) continue;
			entry.socket.send(frame);
		}
	}

	// Une connexion arrive : elle n'a pas encore de session, et n'en aura une
	// qu'après un handshake accepté. D'ici là, rien ne transite.
	#accept(socket) {
		socket.sessionId = null;
		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): nouvelle connexion");

		const guard = setTimeout(() => {
			if (socket.sessionId !== null) return;
			console.error("ERROR (" + this.instanceName + "): pas de handshake après " + this.#handshakeTimeoutMs() + " ms — socket fermé");
			socket.close();
		}, this.#handshakeTimeoutMs());

		socket.on("message", (buffer) => this.#onFrame(socket, buffer));
		socket.on("close", () => {
			clearTimeout(guard);
			this.#disconnected(socket);
		});
		socket.on("error", (error) => console.error("ERROR (" + this.instanceName + "): socket — " + error));
	}

	// Une trame brute. Une trame trop grande, illisible ou arrivée avant le
	// handshake est une violation de protocole : le canal ferme le socket.
	#onFrame(socket, buffer) {
		const maxFrameBytes = this.deps.maxFrameBytes ?? 262144;
		if (buffer.length > maxFrameBytes) {
			console.error("ERROR (" + this.instanceName + "): trame de " + buffer.length + " octets > " + maxFrameBytes + " — socket fermé");
			socket.close();
			return;
		}
		let message;
		try {
			message = JSON.parse(buffer.toString());
		} catch {
			console.error("ERROR (" + this.instanceName + "): trame non-JSON — socket fermé");
			socket.close();
			return;
		}
		if (message === null || typeof message !== "object" || Array.isArray(message)) {
			console.error("ERROR (" + this.instanceName + "): trame qui n'est pas une enveloppe — socket fermé");
			socket.close();
			return;
		}

		if (message.handshake_request !== undefined) {
			this.#handshake(socket, message.handshake_request);
			return;
		}
		if (socket.sessionId === null) {
			console.error("ERROR (" + this.instanceName + "): trame avant handshake — socket fermé");
			socket.close();
			return;
		}
		this.#inbound(socket, message);
	}

	// Un message applicatif entrant, dans l'ordre :
	//   1. estampillage : "id" et "session" posés sur l'enveloppe ;
	//   2. upcast : de la révision que ce client émet vers le latest du serveur ;
	//   3. admission : session.allowMessage, si un composant d'auth l'a posée ;
	//   4. publication : le broker valide le payload contre le catalogue et livre.
	#inbound(socket, message) {
		const entry = this.#sessions.get(socket.sessionId);
		message.id = socket.sessionId;
		message.session = entry.session;
		this.#upcastInbound(message, entry.emitFrom);
		if (this.#admitInbound(message, entry.session)) this.broker.publish(this, message);
	}

	// Le handshake. Le serveur vérifie le contrat de canal du client ; s'il est
	// compatible, il crée la session (ou la reprend si le client présente son
	// jeton) et répond avec le jeton et les baselines ; sinon il répond l'erreur
	// et ferme. Le jeton EST l'identifiant de session : un UUID impossible à
	// deviner, comme un identifiant de session PHP.
	#handshake(socket, request) {
		const verdict = this.#verifyContract(request);
		if (!verdict.ok) {
			console.error("ERROR (" + this.instanceName + "): contrat incompatible — " + verdict.error + " — socket fermé");
			socket.send(JSON.stringify({ handshake_response: {
				fingerprint: this.deps.channel.fingerprint,
				error: verdict.error,
				warnings: verdict.warnings,
			} }));
			socket.close();
			return;
		}
		if (verdict.warnings.length > 0 && (this.broker.verbose || this.verbose)) {
			for (const warning of verdict.warnings) console.warn("WARN (" + this.instanceName + "): " + warning);
		}

		const resumed = request.session_token !== undefined && this.#sessions.has(request.session_token);
		let sessionId;
		let entry;
		if (resumed) {
			sessionId = request.session_token;
			entry = this.#sessions.get(sessionId);
			clearTimeout(entry.expiryTimer);
			entry.expiryTimer = null;
			entry.emitFrom = verdict.emitFrom; // le client a pu être mis à jour entre-temps
			// même jeton présenté deux fois : le nouveau socket prend la place,
			// l'ancien est fermé après l'échange pour que sa fermeture soit ignorée
			const previous = entry.socket;
			entry.socket = socket;
			if (previous !== null && previous !== socket && previous.readyState === WebSocket.OPEN) previous.close();
		} else {
			sessionId = randomUUID();
			entry = { session: { data: {} }, socket, expiryTimer: null, emitFrom: verdict.emitFrom };
			this.#sessions.set(sessionId, entry);
		}
		socket.sessionId = sessionId;

		socket.send(JSON.stringify({ handshake_response: {
			fingerprint: this.deps.channel.fingerprint,
			session_token: sessionId,
			baselines: verdict.baselines,
			warnings: verdict.warnings,
		} }));
		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): session " + sessionId + (resumed ? " reprise" : " créée"));
		this.#publishEvent(resumed ? "resumed" : "connected", sessionId, entry.session);
	}

	// Vérifie le contrat de canal du client. L'analyse ne dépend que du contrat :
	// elle est mémorisée, des clients identiques la partagent. Le millésime
	// (fingerprint) est une étiquette humaine : différent, il vaut un avertissement,
	// jamais un refus.
	#verifyContract(request) {
		const contract = request.contract;
		if (contract === undefined) return { ok: false, error: "handshake sans contrat de canal", warnings: [] };

		const cacheKey = canonicalStringify(contract);
		let analysis = this.#analysisCache.get(cacheKey);
		if (analysis === undefined) {
			analysis = this.#analyzeContract(contract);
			this.#analysisCache.set(cacheKey, analysis);
		}

		const warnings = [...analysis.warnings];
		if (request.fingerprint !== this.deps.channel.fingerprint) {
			warnings.push("millésime différent (client " + request.fingerprint + ", serveur " + this.deps.channel.fingerprint + ")");
		}
		return { ...analysis, warnings };
	}

	// La décision d'alignement, fonction pure du contrat client. Elle refuse :
	//   - un message qui transite d'un côté et pas de l'autre ;
	//   - une version différente pour le même message ;
	//   - une révision partagée dont le schéma diffère (une révision est un
	//     snapshot gravé : si elle a changé, la discipline d'ajout a été rompue) ;
	//   - une révision qui viole la règle directionnelle : le récepteur doit
	//     être au moins aussi à jour que l'émetteur, car il n'y a pas de downcast.
	// Elle rend :
	//   - emitFrom  (client → serveur) : la révision que le client émet, d'où le
	//     serveur upcaste ses trames entrantes ;
	//   - baselines (serveur → client) : la révision que le serveur émet, d'où le
	//     client upcaste les trames qu'il reçoit.
	#analyzeContract(contract) {
		const warnings = [];
		const problems = [];
		const key = (d) => d.name + "@v" + d.version;
		const clientEmits = new Map((contract.clientEmits ?? []).map((d) => [key(d), d]));
		const clientReceives = new Map((contract.clientReceives ?? []).map((d) => [key(d), d]));
		const clientVersions = (name, version) => contract.schemas?.[name]?.versions?.[version];
		const serverVersion = (name, version) => this.broker.catalog[name].versions[version];

		// mêmes couples nom + version des deux côtés, dans chaque direction
		const compareSet = (serverList, clientMap, direction) => {
			const serverKeys = new Set(serverList.map(key));
			for (const d of serverList) if (!clientMap.has(key(d))) problems.push(direction + " : " + d.name + " v" + d.version + " manquant côté client");
			for (const [, d] of clientMap) if (!serverKeys.has(key(d))) problems.push(direction + " : " + d.name + " v" + d.version + " en trop côté client");
		};
		compareSet(this.#clientEmits, clientEmits, "client→serveur");
		compareSet(this.#clientReceives, clientReceives, "serveur→client");

		// pour un message présent des deux côtés : égalité canonique des révisions
		// partagées (0 .. min des latest), puis règle directionnelle sur les latest
		const align = (d, direction, receiverIsServer) => {
			const clientVersion = clientVersions(d.name, d.version);
			if (clientVersion?.revisions === undefined) {
				problems.push(direction + " : " + d.name + " v" + d.version + " sans schéma dans le contrat client");
				return null;
			}
			const serverVer = serverVersion(d.name, d.version);
			const clientLatest = latestRevision(clientVersion);
			const serverLatest = latestRevision(serverVer);
			for (let r = 0; r <= Math.min(clientLatest, serverLatest); r++) {
				if (canonicalStringify(clientVersion.revisions[r]) !== canonicalStringify(serverVer.revisions[r])) {
					problems.push(direction + " : " + d.name + " v" + d.version + " r" + r + " — schéma divergent (discipline append-only rompue ?)");
				}
			}
			const receiver = receiverIsServer ? serverLatest : clientLatest;
			const emitter = receiverIsServer ? clientLatest : serverLatest;
			if (receiver < emitter) {
				problems.push(direction + " " + d.name + " v" + d.version + " : révision récepteur (r" + receiver + ") < émetteur (r" + emitter + ")");
				return null;
			}
			if (clientLatest !== serverLatest) warnings.push(direction + " " + d.name + " : client r" + clientLatest + ", serveur r" + serverLatest + " (upcast récepteur)");
			return { clientLatest, serverLatest };
		};

		const emitFrom = {};
		const baselines = [];
		for (const d of this.#clientEmits) {
			if (!clientEmits.has(key(d))) continue;
			const aligned = align(d, "client→serveur", true);
			if (aligned !== null) emitFrom[d.name] = aligned.clientLatest;
		}
		for (const d of this.#clientReceives) {
			if (!clientReceives.has(key(d))) continue;
			const aligned = align(d, "serveur→client", false);
			if (aligned !== null) baselines.push({ name: d.name, version: d.version, revision: aligned.serverLatest });
		}

		if (problems.length > 0) return { ok: false, error: problems.join(" ; "), warnings };
		return { ok: true, warnings, baselines, emitFrom };
	}

	// Upcaste chaque payload entrant depuis la révision que la session émet
	// (emitFrom) jusqu'au latest du serveur : le broker ne voit que des formes
	// canoniques. Un payload que l'upcast ne sait pas traiter est laissé tel quel,
	// le broker le rejettera à la publication.
	#upcastInbound(message, emitFrom) {
		for (const name of Object.keys(message)) {
			if (name === "id" || name === "session") continue;
			const version = this.#emitVersion.get(name);
			const baseline = emitFrom?.[name];
			if (version === undefined || baseline === undefined) continue;
			try {
				message[name] = upcastToLatestRevision(this.broker.catalog[name].versions[version], baseline, message[name]);
			} catch (exception) {
				console.error("ERROR (" + this.instanceName + "): upcast entrant de " + name + " échoué — laissé tel quel : " + (exception?.message ?? exception));
			}
		}
	}

	// La grille d'admission. Un composant d'authentification pose
	// session.allowMessage(name, payload) au connected ; elle lit l'identité dans
	// session.data (this === session) et décide pour chaque message entrant.
	// Un message refusé est retiré de l'enveloppe avec un avertissement : un
	// client bien construit n'en envoie jamais, un refus est un bug ou une attaque.
	// Rend vrai s'il reste quelque chose à publier.
	#admitInbound(message, session) {
		if (typeof session?.allowMessage !== "function") return true;
		let admitted = false;
		for (const name of Object.keys(message)) {
			if (name === "id" || name === "session") continue;
			if (session.allowMessage(name, message[name])) { admitted = true; continue; }
			console.warn("WARN (" + this.instanceName + "): message '" + name + "' refusé pour la session " + message.id + " (allowMessage) — jeté");
			delete message[name];
		}
		return admitted;
	}

	// Socket fermé : la session survit sessionTtlMs (un rechargement de page ou
	// une coupure réseau la reprendra), puis expire. L'expired est la dernière
	// livraison où la session est lisible : c'est là qu'un composant libère ce
	// qu'il tenait pour elle.
	#disconnected(socket) {
		const sessionId = socket.sessionId;
		if (sessionId === null || sessionId === undefined) return; // jamais de handshake
		const entry = this.#sessions.get(sessionId);
		if (entry === undefined || entry.socket !== socket) return; // déjà reprise sur un autre socket
		entry.socket = null;
		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): session " + sessionId + " déconnectée");
		this.#publishEvent("disconnected", sessionId, entry.session);
		entry.expiryTimer = setTimeout(() => {
			this.#publishEvent("expired", sessionId, entry.session);
			this.#sessions.delete(sessionId);
		}, this.deps.sessionTtlMs ?? 300000);
		entry.expiryTimer.unref?.();
	}

	// L'état d'une session, annoncé comme n'importe quel message : le broker le
	// valide et le livre aux abonnés, avec "id" et "session" sur l'enveloppe.
	#publishEvent(event, sessionId, session) {
		this.broker.publish(this, {
			connection_event: { event, channel: this.instanceName },
			id: sessionId,
			session,
		});
	}

	#handshakeTimeoutMs() {
		return this.deps.handshakeTimeoutMs ?? 10000;
	}

}

export default WebSocketChannel;
