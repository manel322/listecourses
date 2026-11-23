/*
 * public/fw/channel.catalog.js — contracts the framework provides to every
 * CLIENT broker (they never cross the network). Spread them into the client
 * union catalog (catalogs/catalog.js), like the server does with
 * fw/channel.catalog.js.
 * connection_event, client side: each client channel publishes it, under its
 * own instanceName, when its link changes state. A client may mount several
 * channels, so `channel` tells them apart. Same name and shape as the server
 * one, minus "expired" (a session TTL is the server's business) and minus the
 * id/session envelope keys (the client broker ignores them).
 */

export const CLIENT_CHANNEL_CONTRACTS = {

	// channel → client components. "connected" = first handshake done,
	// "disconnected" = the socket dropped (the channel retries by itself),
	// "resumed" = reconnected and re-handshaken, same session token presented.
	connection_event: {
		versions: { 1: { revisions: { 0: { fields: {
			event:   { type: "string", required: true, enum: ["connected", "resumed", "disconnected"] },
			channel: { type: "string", required: true },
		} } } } },
		examples: { 1: { 0: [
			{ event: "connected", channel: "canal_client" },
			{ event: "disconnected", channel: "canal_client" },
			{ event: "resumed", channel: "canal_client" },
		] } },
		counterexamples: { 1: { 0: [
			{ reason: "event hors énumération (expired est serveur-seulement)",
			  payload: { event: "expired", channel: "canal_client" } },
			{ reason: "channel manquant", payload: { event: "connected" } },
		] } },
	},

};

export default CLIENT_CHANNEL_CONTRACTS;
