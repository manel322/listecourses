/*
 * fw/channel.catalog.js — contracts the framework itself provides for every
 * channel (server side only, they never cross the network). Spread them into
 * the server broker's union catalog (catalogs/catalog.js): the broker stays
 * strict, components subscribe to them like to any other message.
 * connection_event is emitted by every server channel at the four session
 * transitions (fwtozza.md § "Les exigences d'un canal"); the envelope carries
 * the session id ("id") and the session object ("session").
 */

export const CHANNEL_CONTRACTS = {

	// channel → server components: a session changed state. "connected" = new
	// session after a successful handshake, "resumed" = a client came back with
	// its token, "disconnected" = socket closed (session kept until the TTL),
	// "expired" = TTL elapsed — the session is destroyed right after this
	// delivery, last chance to read session.data.
	connection_event: {
		versions: {
			1: {
				revisions: {
					0: {
						fields: {
							event:   { type: "string", required: true, enum: ["connected", "resumed", "disconnected", "expired"] },
							channel: { type: "string", required: true },
						},
					},
				},
			},
		},
		examples: { 1: {
			0: [
				{ event: "connected", channel: "channel_game" },
				{ event: "expired", channel: "channel_game" },
			],
		} },
		counterexamples: { 1: {
			0: [
				{ reason: "event outside enum",
				  payload: { event: "left", channel: "channel_game" } },
				{ reason: "channel missing",
				  payload: { event: "connected" } },
			],
		} },
	},

};

export default CHANNEL_CONTRACTS;
