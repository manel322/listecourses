/*
 * fw/broker.js — fwtozza message broker (Node side).
 * Components register with the broker, which publishes their messages to
 * subscribers after validating them against the catalog (engine.js).
 * Contract enforcement is strict: a declaration unknown to the catalog is
 * refused at registration; an invalid payload is dropped at publication.
 * Emitters declare the revision they produce (default: latest); subscribers
 * declare only the version they consume and always receive the
 * latest-revision form of that version (§6.4). Version gaps are bridged by
 * the declarative upcasters of the catalog (upcastFrom, §2.2.d) — upward
 * only, no downcast (§2.2.b).
 * Handles the reserved envelope keys "id" (session targeting, §6.2) and
 * "session" (per-session state carried by a channel): both are skipped as
 * message names and joined to every delivery — "id" by value, "session" BY
 * REFERENCE (deliberately not cloned: every subscriber sees the same session
 * object, so a component writing identity into session.data makes it visible
 * to all later deliveries of that session). The payload itself stays a
 * private copy per subscriber.
 * Delivery is a run-to-completion FIFO queue: validation happens on the
 * emitter's stack at publish time, then the message is queued; a publish
 * issued from inside a handleMessage is queued too and delivered AFTER the
 * current message has been served to all its subscribers — never nested.
 * A subscriber throwing never deprives the others (per-delivery try/catch),
 * and a delivery budget per drain turns publish cycles into a logged error
 * instead of a freeze.
 * audit() adds a read-only, composition-time look at the resulting graph
 * (§ "L'audit du graphe"): it returns the drifts between what the catalog
 * describes and what the mounted code actually wires — severity "error" for a
 * wire that will never carry anything, "warning" for a version or revision gap
 * the broker absorbs by upcast — and decides nothing.
 */

import { validate, validateCatalog, upcastToLatestRevision, completeToLatestRevision, latestRevision, upcast } from "./engine.js";

class Broker {

	catalog;
	verbose = false;

	// per message name: { [instanceName]: { object, version, revision } }
	emitters = {};
	// per message name: { [instanceName]: { object, version } }
	subscribers = {};

	// Ceiling of messages delivered in a single drain: beyond it, the queue is
	// assumed to feed itself (publish cycle) and is dropped with an error.
	static DELIVERY_BUDGET = 1000;

	// validated messages waiting for delivery: { name, emittedVersion, byVersion[, id] }
	#queue = [];
	// true while #drain empties the queue — a publish arriving then only enqueues
	#draining = false;

	constructor(catalog, { verbose = false } = {}) {
		this.catalog = catalog;
		this.verbose = verbose;
		// structural lint at composition time: defects are loud but not fatal —
		// the application keeps running, and publications touching a defective
		// contract will be dropped by validation
		for (const defect of validateCatalog(catalog)) this.#error(`catalog defect — ${defect}`);
	}

	// text may be a thunk: hot-path logs (one per publish, one per delivery)
	// pass a function so their formatting — JSON.stringify included — costs
	// nothing unless verbose is on
	#info(text) {
		if (this.verbose) console.log("INFO (broker): " + (typeof text === "function" ? text() : text));
	}

	#error(text) {
		console.error("ERROR (broker): " + text);
	}

	// Checks a declaration against the catalog. Returns { name, version[, revision] }
	// or null when refused. Emitters may omit revision: latest is assumed.
	#checkedDeclaration(declaration, isEmit) {
		const { name, version } = declaration ?? {};
		if (typeof name !== "string" || version === undefined) {
			this.#error(`declaration must be { name, version${isEmit ? "[, revision]" : ""} }, got ${JSON.stringify(declaration)}`);
			return null;
		}
		const versionSchema = this.catalog[name]?.versions?.[version];
		if (versionSchema === undefined) {
			this.#error(`${name} v${version} is not in the catalog`);
			return null;
		}
		if (!isEmit) return { name, version: Number(version) };
		const revision = declaration.revision ?? latestRevision(versionSchema);
		if (versionSchema.revisions[revision] === undefined) {
			this.#error(`${name} v${version} r${revision} is not in the catalog`);
			return null;
		}
		return { name, version: Number(version), revision: Number(revision) };
	}

	// Registration is ATOMIC per object: every declaration is validated first
	// (malformed declaration, unknown contract/version/revision, instanceName
	// collision — with the registry or within this very call) and a single
	// defect rejects the WHOLE object: nothing gets registered, the errors are
	// logged, the application keeps running without the object. Returns true
	// when the object is registered, false when it was rejected.
	register(object, emits = [], receives = []) {
		if (typeof object?.instanceName !== "string" || object.instanceName === "") {
			this.#error(`registration rejected — object without instanceName`);
			return false;
		}
		const instanceName = object.instanceName;
		const plannedEmits = [];
		const plannedReceives = [];
		let valid = true;
		for (const declaration of emits) {
			const d = this.#checkedDeclaration(declaration, true);
			if (d === null) { valid = false; continue; }
			if (this.emitters[d.name]?.[instanceName] !== undefined || plannedEmits.some((p) => p.name === d.name)) {
				this.#error(`${instanceName} is already registered as emitter of ${d.name}`);
				valid = false;
				continue;
			}
			plannedEmits.push(d);
		}
		for (const declaration of receives) {
			const d = this.#checkedDeclaration(declaration, false);
			if (d === null) { valid = false; continue; }
			if (this.subscribers[d.name]?.[instanceName] !== undefined || plannedReceives.some((p) => p.name === d.name)) {
				this.#error(`${instanceName} is already registered as subscriber of ${d.name}`);
				valid = false;
				continue;
			}
			plannedReceives.push(d);
		}
		if (!valid) {
			this.#error(`registration of ${instanceName} rejected as a whole — nothing registered (see errors above)`);
			return false;
		}
		for (const d of plannedEmits) {
			this.emitters[d.name] ??= {};
			this.emitters[d.name][instanceName] = { object, version: d.version, revision: d.revision };
			this.#info(`${instanceName} emits ${d.name} v${d.version} r${d.revision}`);
		}
		for (const d of plannedReceives) {
			this.subscribers[d.name] ??= {};
			this.subscribers[d.name][instanceName] = { object, version: d.version };
			this.#info(`${instanceName} receives ${d.name} v${d.version}`);
		}
		return true;
	}

	// Identity is enforced like in publish: the entry must belong to THIS object,
	// not merely bear the same instanceName — a homonym cannot unregister another.
	unregister(object, emits = [], receives = []) {
		for (const { name } of emits) {
			const registration = this.emitters[name]?.[object.instanceName];
			if (registration === undefined) {
				this.#error(`${object.instanceName} was not registered as emitter of ${name}`);
				continue;
			}
			if (registration.object !== object) {
				this.#error(`${name}: unregister by ${object.instanceName} refused — the registration belongs to another object (homonym)`);
				continue;
			}
			delete this.emitters[name][object.instanceName];
			this.#info(`${object.instanceName} no longer emits ${name}`);
		}
		for (const { name } of receives) {
			const subscription = this.subscribers[name]?.[object.instanceName];
			if (subscription === undefined) {
				this.#error(`${object.instanceName} was not registered as subscriber of ${name}`);
				continue;
			}
			if (subscription.object !== object) {
				this.#error(`${name}: unregister by ${object.instanceName} refused — the subscription belongs to another object (homonym)`);
				continue;
			}
			delete this.subscribers[name][object.instanceName];
			this.#info(`${object.instanceName} no longer receives ${name}`);
		}
	}

	// Composition-time diagnostic (fwtozza.md § "L'audit du graphe"): called once
	// everything is mounted, it only READS — publishes nothing, changes nothing —
	// and RETURNS the anomalies instead of deciding for the application: during
	// construction an orphan is normal, the contract is carved before its
	// consumer is written.
	// `transits` is the list of the CHANNEL trios of the mounted channels, and it
	// is indispensable: without it every message crossing a channel looks like an
	// orphan, since its only local subscriber IS the channel. A name quoted by any
	// transit authorization — in either direction — is therefore never reported as
	// orphan_emit or dead_receive.
	// Deliberately blind to the other side of the channel, to payloads, and to
	// unmounted components: this describes the application that runs, and the
	// drift it measures is the one of the GRAPH, not of the content.
	// Each anomaly is { severity, code, name, message, ...context }, in catalog
	// order. "error" = a wire that will never carry anything; "warning" = a drift
	// the broker absorbs at runtime (an upcast bridges it), informational only.
	audit(transits = []) {
		const anomalies = [];
		// message names allowed to cross at least one channel, either direction
		const crossesAChannel = new Set();
		for (const channel of transits ?? []) {
			for (const declaration of [...(channel?.clientEmits ?? []), ...(channel?.clientReceives ?? [])]) {
				if (typeof declaration?.name === "string") crossesAChannel.add(declaration.name);
			}
		}
		for (const name of Object.keys(this.catalog)) {
			const emitters = Object.entries(this.emitters[name] ?? {});
			const subscribers = Object.entries(this.subscribers[name] ?? {});
			const emitterNames = emitters.map(([instanceName]) => instanceName);
			const subscriberNames = subscribers.map(([instanceName]) => instanceName);
			if (emitters.length === 0 && subscribers.length === 0) {
				anomalies.push({ severity: "error", code: "unused_contract", name,
					message: `${name} is in the catalog, neither emitted nor received by anyone` });
				continue; // nothing else to say about a contract nobody cites
			}
			if (subscribers.length === 0 && !crossesAChannel.has(name)) {
				anomalies.push({ severity: "error", code: "orphan_emit", name, emitters: emitterNames,
					message: `${name} is emitted by ${emitterNames.join(", ")}, has no local subscriber and crosses no channel` });
			}
			if (emitters.length === 0 && !crossesAChannel.has(name)) {
				anomalies.push({ severity: "error", code: "dead_receive", name, subscribers: subscriberNames,
					message: `${name} is received by ${subscriberNames.join(", ")}, has no local emitter and crosses no channel — the subscriber looks alive` });
			}
			// An emitter left behind by its own contract: it still produces an older
			// revision than the carved one. Nothing breaks — the broker completes the
			// payload to the current revision before delivering — but the code and
			// the catalog have drifted apart, which is what the audit is here to show.
			for (const [emitterName, registration] of emitters) {
				const currentRevision = latestRevision(this.catalog[name].versions[registration.version]);
				if (registration.revision === currentRevision) continue;
				anomalies.push({ severity: "warning", code: "revision_gap", name, emitter: emitterName,
					version: registration.version, emittedRevision: registration.revision, currentRevision,
					message: `${name}: ${emitterName} emits v${registration.version} r${registration.revision} while the current revision is r${currentRevision} — subscribers receive the completed r${currentRevision} form` });
			}
			// Emitter and subscriber on two different versions. Bridged by the
			// catalog upcasters, it is a drift worth naming and nothing more;
			// unbridged, the pair is a dead branch of the graph, dropped one message
			// at a time by #payloadForVersion at delivery.
			for (const [emitterName, registration] of emitters) {
				for (const [subscriberName, subscription] of subscribers) {
					if (subscription.version === registration.version) continue;
					const blocker = this.#missingUpcastPath(name, registration.version, subscription.version);
					anomalies.push({
						severity: blocker === null ? "warning" : "error",
						code: "version_gap", name, emitter: emitterName, subscriber: subscriberName,
						emittedVersion: registration.version, expectedVersion: subscription.version,
						message: `${name}: ${emitterName} emits v${registration.version}, ${subscriberName} expects v${subscription.version} — `
							+ (blocker === null ? "bridged by the catalog upcasters, delivery unaffected" : `${blocker}, they will never meet`) });
				}
			}
		}
		return anomalies;
	}

	publish(emitter, message) {
		this.#info(() => `received from ${emitter.instanceName}: ${JSON.stringify(message)}`);
		for (const name in message) {
			if (name === "id" || name === "session") continue; // reserved envelope keys, not message names
			const registration = this.emitters[name]?.[emitter.instanceName];
			if (registration === undefined || registration.object !== emitter) {
				this.#error(`${name} published by ${emitter.instanceName} without emitter registration — dropped`);
				continue;
			}
			const versionSchema = this.catalog[name].versions[registration.version];
			const errors = validate(versionSchema, registration.revision, message[name]);
			if (errors.length > 0) {
				this.#error(`${name} v${registration.version} r${registration.revision} from ${emitter.instanceName} invalid — dropped:\n  - ${errors.join("\n  - ")}`);
				continue;
			}
			// canonical form of the emitted version, then one conversion per consumed version
			const byVersion = {
				[registration.version]: upcastToLatestRevision(versionSchema, registration.revision, message[name]),
			};
			// validated on the emitter's stack, delivered by the drain (FIFO)
			this.#queue.push({ name, emittedVersion: registration.version, byVersion, id: message.id, session: message.session });
		}
		this.#drain();
	}

	// Run-to-completion: the outermost publish empties the queue before giving
	// the hand back; a publish issued from a handleMessage finds #draining true
	// and returns right after enqueueing — its message goes AFTER the current
	// one, never inside it. Deterministic FIFO order, bounded stack.
	#drain() {
		if (this.#draining) return;
		this.#draining = true;
		let budget = Broker.DELIVERY_BUDGET;
		try {
			while (this.#queue.length > 0) {
				if (budget-- === 0) {
					const next = this.#queue.slice(0, 5).map((entry) => entry.name).join(" -> ");
					this.#error(`delivery budget exhausted (${Broker.DELIVERY_BUDGET} messages in one drain) — probable publish cycle, queue dropped (next: ${next} ...)`);
					this.#queue.length = 0;
					break;
				}
				this.#deliver(this.#queue.shift());
			}
		} finally {
			this.#draining = false;
		}
	}

	// Serves one queued message to all its subscribers. Subscribers are
	// resolved here (delivery time), not at publish time.
	#deliver({ name, emittedVersion, byVersion, id, session }) {
		for (const [subscriberName, subscription] of Object.entries(this.subscribers[name] ?? {})) {
			// unregister is immediate: an object unregistered during this very
			// delivery (by itself or another subscriber) receives nothing more
			if (this.subscribers[name]?.[subscriberName] !== subscription) continue;
			const payload = this.#payloadForVersion(name, byVersion, emittedVersion, subscription.version);
			if (payload === null) {
				this.#error(`${name}: no upcast path v${emittedVersion} -> v${subscription.version}, ${subscriberName} skipped`);
				continue;
			}
			const delivery = { [name]: structuredClone(payload) }; // each subscriber gets its own copy
			if (id !== undefined) delivery.id = id;
			if (session !== undefined) delivery.session = session; // BY REFERENCE: shared session state, never cloned
			this.#info(() => `delivering ${name} v${subscription.version} to ${subscriberName}`);
			try {
				subscription.object.handleMessage(delivery);
			} catch (exception) {
				// a faulty subscriber never deprives the others
				this.#error(`${subscriberName} threw while handling ${name} — remaining deliveries continue:\n${exception?.stack ?? exception}`);
			}
		}
	}

	// Walks the declarative upcasters from the emitted version up to the target
	// one, caching each step in byVersion. Returns null when no path exists
	// (missing upcastFrom, or a downcast — unsupported by default, §2.2.b).
	#payloadForVersion(name, byVersion, emittedVersion, targetVersion) {
		if (byVersion[targetVersion] !== undefined) return byVersion[targetVersion];
		if (targetVersion < emittedVersion) return null;
		let payload = byVersion[emittedVersion];
		for (let v = emittedVersion + 1; v <= targetVersion; v++) {
			const versionSchema = this.catalog[name].versions[v];
			const rule = versionSchema?.upcastFrom;
			if (rule === undefined) return null;
			// the upcaster was written against the revisions of its time: its
			// output is completed against the CURRENT revision of the target
			// version, so subscribers always receive the canonical form
			payload = completeToLatestRevision(versionSchema, upcast(rule, payload));
			byVersion[v] = payload;
		}
		return payload;
	}

	// Static mirror of #payloadForVersion's walk, payload aside: returns the
	// reason why no upcast path leads from the emitted version to the consumed
	// one, or null when the broker would be able to serve that subscriber.
	#missingUpcastPath(name, emittedVersion, targetVersion) {
		if (targetVersion === emittedVersion) return null;
		if (targetVersion < emittedVersion) return "downcast unsupported";
		for (let v = emittedVersion + 1; v <= targetVersion; v++) {
			if (this.catalog[name].versions[v]?.upcastFrom === undefined) return `v${v} carries no upcastFrom`;
		}
		return null;
	}

}

export default Broker;
