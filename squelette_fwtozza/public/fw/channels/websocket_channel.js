/*
 * public/fw/channels/websocket_channel.js — canal WebSocket, moitié client.
 *
 * Un canal est un objet standard : (broker, instanceName, deps). Il est le seul
 * objet qui connaisse le transport ; les écrans, eux, ne voient que le broker.
 *
 *   deps.socket              le WebSocket déjà créé par le point de composition
 *                            (ouvert ou en cours d'ouverture) — la ressource, pas son URL
 *   deps.channel             le CHANNEL du catalogue de canal : clientEmits / clientReceives
 *   deps.reconnect           facultatif : () => WebSocket, appelé par le canal pour
 *                            rouvrir le lien après une coupure. Absent, pas de reconnexion.
 *   deps.storage             facultatif : getItem / setItem (sessionStorage par exemple),
 *                            conserve le jeton de session à travers un rechargement de page
 *   deps.handshakeTimeoutMs  délai pour obtenir la réponse du serveur (défaut 5 s)
 *
 * Inscription en miroir croisé de CHANNEL : le canal émet localement ce que le
 * client reçoit, et reçoit localement ce que le client émet — plus
 * connection_event, le contrat du framework qui annonce l'état du lien.
 * L'inscription n'a lieu qu'après un handshake accepté : avant, le canal n'existe
 * pas pour le broker, et c'est pourquoi index.html attend ready() avant de monter
 * ce qui parle au serveur.
 *
 * Protocole :
 *   1. handshake_request  (client → serveur) : millésime, listes de transit et
 *      schémas complets des messages qui traversent, jeton de session éventuel.
 *   2. handshake_response (serveur → client) : jeton de session et « baselines »
 *      (la révision que le serveur émet pour chaque message serveur→client), ou
 *      une erreur si les catalogues sont incompatibles — le canal se termine.
 *   3. Ensuite, chaque trame est une enveloppe fwtozza : { nom_du_message: payload }.
 *
 * Le serveur juge la COMPATIBILITÉ des catalogues, pas leur identité. Il peut
 * tourner sur un catalogue plus ancien : le canal upcaste alors chaque trame
 * entrante depuis la baseline reçue jusqu'au latest du client, avant de la
 * publier. Le broker local valide à la publication.
 *
 * Reconnexion : après un premier handshake réussi, une coupure déclenche une
 * réouverture automatique (attente 1 s, 2 s, 4 s… plafonnée à 30 s) et un nouveau
 * handshake avec le même jeton : le serveur reprend la session (resumed). Les
 * messages émis pendant la coupure sont perdus, avec un avertissement : ce sont
 * les composants qui rejouent leur état sur connection_event.
 *
 * Pas de destroy() : un canal ne se démonte pas. close() arrête seulement la
 * boucle de reconnexion que le canal a lui-même lancée.
 */

import { upcastToLatestRevision } from "../engine.js";

const CONNECTION_EVENT = { name: "connection_event", version: 1 };
const RECONNECT_MIN_MS = 1000;
const RECONNECT_MAX_MS = 30000;

class WebSocketChannel {

	instanceName;
	broker;
	deps;
	socket;
	verbose = false;

	emits;
	receives;

	#clientEmits;      // client → serveur : [{ name, version }]
	#clientReceives;   // serveur → client : [{ name, version }]
	#baselines = new Map(); // nom → { version, revision } que le serveur émet, apprises au handshake

	#storageKey;
	#closing = false;
	#timer = null;
	#delay = RECONNECT_MIN_MS;
	#isReady = false; // premier handshake réussi et inscription faite
	#live = false;    // handshake réussi sur le socket COURANT (faux pendant une reconnexion)
	#resolveReady;
	#rejectReady;
	#ready = new Promise((resolve, reject) => { this.#resolveReady = resolve; this.#rejectReady = reject; });

	constructor(broker, instanceName, deps = {}) {
		this.broker = broker;
		this.instanceName = instanceName;
		this.deps = deps;
		this.#clientEmits = deps.channel.clientEmits;
		this.#clientReceives = deps.channel.clientReceives;
		this.#storageKey = "fwtozza:session:" + instanceName;

		// miroir croisé : ce que le client reçoit, le canal l'émet ici ; ce que le
		// client émet, le canal le reçoit ici pour l'envoyer. Sans révision :
		// seules des formes canoniques circulent entre les deux brokers.
		this.emits = [...this.#clientReceives, CONNECTION_EVENT];
		this.receives = [...this.#clientEmits];

		this.#ready.catch(() => {}); // une application qui n'attend pas ready() ne doit pas produire de rejet non géré
		this.#attach(deps.socket);
	}

	// Résolue après le premier handshake accepté ; rejetée si le contrat est
	// incompatible, si la connexion se ferme avant l'accord, si le broker refuse
	// l'inscription ou si le délai est écoulé.
	ready() {
		const timeoutMs = this.deps.handshakeTimeoutMs ?? 5000;
		return Promise.race([
			this.#ready,
			new Promise((resolve, reject) => setTimeout(() => {
				if (this.#isReady) return;
				this.socket.close();
				reject(new Error("timeout du handshake après " + timeoutMs + " ms"));
			}, timeoutMs)),
		]);
	}

	// Sortant : une émission d'un écran à destination du serveur. L'enveloppe
	// part telle quelle ; sans lien établi, elle est perdue avec un avertissement.
	handleMessage(message) {
		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): reçoit " + JSON.stringify(message));
		if (this.#live && this.socket.readyState === WebSocket.OPEN) {
			this.socket.send(JSON.stringify(message));
			return;
		}
		console.warn("WARN (" + this.instanceName + "): " + Object.keys(message).join(", ") + " perdu, lien coupé");
	}

	// Arrête la boucle de reconnexion et ferme le lien courant.
	close() {
		this.#closing = true;
		if (this.#timer !== null) { clearTimeout(this.#timer); this.#timer = null; }
		this.socket.close();
	}

	// Branche le canal sur un socket. Le handshake se refait à chaque socket, avec
	// le jeton mémorisé ; l'inscription au broker, elle, n'a lieu qu'une fois.
	#attach(socket) {
		this.socket = socket;
		this.#live = false;

		const hello = () => socket.send(JSON.stringify({ handshake_request: this.#handshakeRequest() }));
		if (socket.readyState === WebSocket.OPEN) hello();
		else socket.addEventListener("open", hello, { once: true });

		socket.addEventListener("message", (event) => {
			if (socket !== this.socket) return; // un socket remplacé ne parle plus
			let message;
			try {
				message = JSON.parse(event.data);
			} catch (exception) {
				console.error("ERROR (" + this.instanceName + "): trame illisible", exception);
				return;
			}
			if (message.handshake_response !== undefined) {
				this.#onHandshakeResponse(socket, message.handshake_response);
				return;
			}
			if (!this.#isReady || !this.#live) return; // rien ne transite avant le handshake
			this.#upcastInbound(message);
			this.broker.publish(this, message); // le broker valide et livre aux écrans
		});

		socket.addEventListener("error", (event) => {
			if (socket !== this.socket) return;
			console.error("ERROR (" + this.instanceName + "): socket — " + (event.message ?? "erreur"));
			// connexion impossible avant le handshake : ready() échoue tout de suite
			// (certains hôtes n'émettent pas de close après un échec d'ouverture)
			if (!this.#isReady) this.#rejectReady(new Error("connexion impossible : " + (event.message ?? "erreur réseau")));
		});

		socket.addEventListener("close", () => {
			if (socket !== this.socket) return;
			console.warn("WARN (" + this.instanceName + "): connexion fermée");
			if (!this.#isReady) { this.#rejectReady(new Error("connexion fermée avant le handshake")); return; }
			this.#live = false;
			this.#signal("disconnected");
			this.#scheduleReconnect();
		});
	}

	// Ce que le client présente au serveur : les deux listes de transit et, pour
	// chaque message qui traverse, ses schémas complets (l'objet versions du
	// catalogue local ; les exemples n'en font pas partie). connection_event
	// reste local : il n'est pas décrit.
	#handshakeRequest() {
		const list = (declarations) => declarations.map(({ name, version }) => ({ name, version }));
		const schemas = {};
		for (const { name } of [...this.#clientEmits, ...this.#clientReceives]) {
			schemas[name] = { versions: this.broker.catalog[name].versions };
		}
		const request = {
			fingerprint: this.deps.channel.fingerprint,
			contract: { clientEmits: list(this.#clientEmits), clientReceives: list(this.#clientReceives), schemas },
		};
		const token = this.#storedToken();
		if (token !== null) request.session_token = token; // reprise de session
		return request;
	}

	// La réponse du serveur. Une erreur termine le canal : un refus ne se
	// réessaie pas. Sinon on mémorise jeton et baselines, puis, au premier
	// handshake seulement, on s'inscrit au broker.
	#onHandshakeResponse(socket, handshake) {
		if (handshake.error !== undefined) {
			console.error("ERROR (" + this.instanceName + "): handshake refusé — " + handshake.error + " — canal terminé");
			this.#closing = true;
			socket.close();
			this.#rejectReady(new Error(handshake.error));
			return;
		}
		for (const warning of handshake.warnings ?? []) console.warn("WARN (" + this.instanceName + "): " + warning);
		this.#baselines.clear();
		for (const b of handshake.baselines ?? []) this.#baselines.set(b.name, { version: b.version, revision: b.revision });
		this.#storeToken(handshake.session_token);
		this.#delay = RECONNECT_MIN_MS; // lien sain : le prochain retard repart du minimum
		this.#live = true;

		if (this.#isReady) { this.#signal("resumed"); return; }

		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): register après handshake");
		if (!this.broker.register(this, this.emits, this.receives)) {
			// inscription refusée en bloc (un message du CHANNEL manque au catalogue ?) :
			// le canal se termine avec la raison, comme sur un handshake refusé
			console.error("ERROR (" + this.instanceName + "): inscription rejetée par le broker — canal inactif");
			this.#closing = true;
			socket.close();
			this.#rejectReady(new Error("inscription rejetée par le broker"));
			return;
		}
		this.#isReady = true;
		this.#resolveReady();
		this.#signal("connected");
	}

	// Upcaste chaque payload entrant depuis la révision que le serveur émet
	// (baseline) jusqu'au latest du client : le broker ne voit que des formes
	// canoniques. Un payload que l'upcast ne sait pas traiter est laissé tel quel,
	// le broker le rejettera à la publication.
	#upcastInbound(message) {
		for (const name of Object.keys(message)) {
			const baseline = this.#baselines.get(name);
			if (baseline === undefined) continue;
			try {
				message[name] = upcastToLatestRevision(this.broker.catalog[name].versions[baseline.version], baseline.revision, message[name]);
			} catch (exception) {
				console.error("ERROR (" + this.instanceName + "): upcast entrant de " + name + " échoué — laissé tel quel : " + (exception?.message ?? exception));
			}
		}
	}

	// Reconnexion avec attente croissante, seulement si le point de composition
	// a fourni deps.reconnect, jamais après close() ni après un refus.
	#scheduleReconnect() {
		if (this.#closing || this.#timer !== null) return;
		if (typeof this.deps.reconnect !== "function") {
			console.warn("WARN (" + this.instanceName + "): pas de deps.reconnect — le lien ne sera pas rouvert");
			return;
		}
		const delay = this.#delay;
		this.#delay = Math.min(this.#delay * 2, RECONNECT_MAX_MS);
		console.warn("WARN (" + this.instanceName + "): nouvelle tentative dans " + Math.round(delay / 1000) + " s");
		this.#timer = setTimeout(() => {
			this.#timer = null;
			if (!this.#closing) this.#attach(this.deps.reconnect());
		}, delay);
	}

	// L'état du lien, annoncé comme n'importe quel message : le broker le valide
	// et le livre aux écrans abonnés. Le canal ne connaît aucun composant.
	#signal(event) {
		this.broker.publish(this, { connection_event: { event, channel: this.instanceName } });
	}

	// Le jeton de session vit dans deps.storage s'il est fourni (sessionStorage :
	// il survit au rechargement de page, pas à l'onglet), sinon le temps du canal.
	#token = null;

	#storedToken() {
		try { return this.deps.storage?.getItem(this.#storageKey) ?? this.#token; } catch { return this.#token; }
	}

	#storeToken(token) {
		this.#token = token;
		try { this.deps.storage?.setItem(this.#storageKey, token); } catch { /* stockage indisponible : le jeton reste en mémoire */ }
	}

}

export default WebSocketChannel;
