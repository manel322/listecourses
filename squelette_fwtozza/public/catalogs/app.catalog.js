/*
 * public/catalogs/app.catalog.js — le catalogue du canal « app ».
 * SHARED : les contrats client ↔ serveur. CHANNEL : ce qui a le droit de traverser.
 * Les deux moitiés du canal importent CE fichier.
 */
export const FINGERPRINT = "app-1";

export const SHARED = {

	// exemple à remplacer : client → serveur
	ping: {
		versions: { 1: { revisions: { 0: { fields: {
			texte: { type: "string", required: true, pattern: "^.{1,100}$" },
		} } } } },
		examples:        { 1: { 0: [ { texte: "bonjour" } ] } },
		counterexamples: { 1: { 0: [ { reason: "texte manquant", payload: {} } ] } },
	},

	// exemple à remplacer : serveur → client
	pong: {
		versions: { 1: { revisions: { 0: { fields: {
			texte: { type: "string", required: true },
		} } } } },
		examples:        { 1: { 0: [ { texte: "BONJOUR" } ] } },
		counterexamples: { 1: { 0: [ { reason: "texte manquant", payload: {} } ] } },
	},

};

export const CHANNEL = {
	fingerprint: FINGERPRINT,
	clientEmits:    [{ name: "ping", version: 1 }],
	clientReceives: [{ name: "pong", version: 1 }],
};
export default CHANNEL;
