/*
 * public/fw/engine.js — fwtozza generic engine.
 * Shared by all projects, never regenerated: interprets catalog schemas (pure data).
 * Lives under public/ so the browser can import it; the server imports the
 * re-export shim fw/engine.js — single source, no drift between the two sides.
 * Vocabulary: fields / required / pattern / enum / default / items / oneOf.
 * Rules: FEUILLE_DE_ROUTE_FWTOZZA.md §6 (R1..R6), glossary §7.
 */

const TYPE_CHECKS = {
	string:  (v) => typeof v === "string",
	number:  (v) => typeof v === "number" && Number.isFinite(v),
	boolean: (v) => typeof v === "boolean",
	object:  (v) => typeof v === "object" && v !== null && !Array.isArray(v),
	array:   (v) => Array.isArray(v),
};

// Structural lint of a whole catalog, run once at broker construction.
// Returns an array of error strings (empty = sound). Checks everything the
// runtime silently relies on: well-formed versions/revisions/fields, known
// types, object rules carrying "fields" and array rules carrying "items" (R4),
// pattern only on strings (and compiling), enum matching the declared
// primitive type, defaults valid against their own rule, oneOf referencing
// declared fields, upcastFrom well-formed — plus the canonical-form guarantee:
// a field the LATEST revision requires but that an earlier revision could
// omit must carry a default, or upcasting can never complete it (§6.4).
export function validateCatalog(catalog) {
	if (!TYPE_CHECKS.object(catalog)) return ["catalog: object expected"];
	const errors = [];
	for (const [contractName, contract] of Object.entries(catalog)) {
		if (!TYPE_CHECKS.object(contract?.versions) || Object.keys(contract.versions).length === 0) {
			errors.push(`${contractName}: "versions" object expected`);
			continue;
		}
		for (const [v, versionSchema] of Object.entries(contract.versions)) {
			const where = `${contractName} v${v}`;
			if (!TYPE_CHECKS.object(versionSchema?.revisions) || Object.keys(versionSchema.revisions).length === 0) {
				errors.push(`${where}: "revisions" object expected`);
				continue;
			}
			if (versionSchema.upcastFrom !== undefined) errors.push(...checkUpcaster(versionSchema.upcastFrom, where));
			for (const [r, revision] of Object.entries(versionSchema.revisions)) {
				errors.push(...checkSchema(revision, `${where} r${r}`));
			}
			const latest = versionSchema.revisions[latestRevision(versionSchema)];
			if (TYPE_CHECKS.object(latest?.fields)) {
				for (const [r, revision] of Object.entries(versionSchema.revisions)) {
					if (Number(r) === latestRevision(versionSchema)) continue;
					errors.push(...checkCompletable(latest.fields, revision.fields ?? {}, `${where} r${r}`, ""));
				}
			}
		}
	}
	return errors;
}

// schema = { fields, oneOf? } — revision root or nested object rule.
function checkSchema(schema, where) {
	if (!TYPE_CHECKS.object(schema?.fields)) return [`${where}: "fields" object expected (R4)`];
	const errors = [];
	// field names must not contain "." — upcastFrom paths use "." as their separator,
	// so a dotted name would be unreachable/ambiguous for rename/defaults/remove
	for (const name of Object.keys(schema.fields)) {
		if (name.includes(".")) errors.push(`${where}: field name "${name}" must not contain "." (reserved as upcastFrom path separator)`);
	}
	if (schema.oneOf !== undefined) {
		if (!TYPE_CHECKS.array(schema.oneOf) || schema.oneOf.some((k) => !(k in schema.fields))) {
			errors.push(`${where}: "oneOf" must be an array of declared field names`);
		} else {
			// a oneOf member is mutually exclusive, so it must stay optional and
			// default-less: upcast is oneOf-oblivious and would fill a required+default
			// member, breaking the "exactly one" invariant (never re-checked after upcast)
			for (const k of schema.oneOf) {
				if (schema.fields[k].required) errors.push(`${where}: oneOf member "${k}" must not be required`);
				if (schema.fields[k].default !== undefined) errors.push(`${where}: oneOf member "${k}" must not have a default`);
			}
		}
	}
	for (const [name, rule] of Object.entries(schema.fields)) {
		errors.push(...checkRule(rule, `${where}.${name}`));
	}
	return errors;
}

function checkRule(rule, where) {
	if (!TYPE_CHECKS.object(rule)) return [`${where}: rule object expected`];
	const check = TYPE_CHECKS[rule.type];
	if (!check) return [`${where}: unknown type "${rule.type}"`];
	const errors = [];
	if (rule.pattern !== undefined) {
		if (rule.type !== "string") errors.push(`${where}: "pattern" only applies to strings`);
		else try { new RegExp(rule.pattern); } catch { errors.push(`${where}: "pattern" does not compile`); }
	}
	if (rule.enum !== undefined) {
		if (rule.type === "object" || rule.type === "array") {
			errors.push(`${where}: "enum" only applies to primitives`);
		} else if (!TYPE_CHECKS.array(rule.enum) || rule.enum.length === 0 || rule.enum.some((value) => !check(value))) {
			errors.push(`${where}: "enum" must be a non-empty array of ${rule.type} values`);
		}
	}
	if (rule.type === "object") {
		errors.push(...checkSchema(rule, where));
	}
	if (rule.type === "array") {
		if (rule.items === undefined) errors.push(`${where}: array rule without "items" (R4)`);
		else errors.push(...checkRule(rule.items, `${where}[]`));
	}
	if (rule.default !== undefined) {
		const defaultErrors = validateValue(rule, rule.default, `${where}.default`);
		errors.push(...defaultErrors);
	}
	return errors;
}

function checkUpcaster(upcastRule, where) {
	const errors = [];
	if (!TYPE_CHECKS.object(upcastRule)) return [`${where}: "upcastFrom" object expected`];
	if (upcastRule.rename !== undefined && !TYPE_CHECKS.object(upcastRule.rename)) errors.push(`${where}: upcastFrom "rename" object expected`);
	if (upcastRule.defaults !== undefined && !TYPE_CHECKS.object(upcastRule.defaults)) errors.push(`${where}: upcastFrom "defaults" object expected`);
	if (upcastRule.remove !== undefined && !TYPE_CHECKS.array(upcastRule.remove)) errors.push(`${where}: upcastFrom "remove" array expected`);
	return errors;
}

// The canonical-form guarantee behind upcastFields: whatever the latest
// revision requires and an earlier revision could legitimately omit must be
// completable, i.e. carry a default.
function checkCompletable(latestFields, earlierFields, where, path) {
	const errors = [];
	for (const [name, rule] of Object.entries(latestFields)) {
		const earlier = earlierFields[name];
		const omittable = earlier === undefined || !earlier.required;
		if (rule.required && omittable && rule.default === undefined) {
			errors.push(`${where}: "${path}${name}" is required in the current revision but omittable here and has no default`
				+ " — the canonical form cannot be completed (§6.4)");
		}
		if (earlier === undefined) continue;
		if (rule.type === "object" && TYPE_CHECKS.object(rule.fields)) {
			errors.push(...checkCompletable(rule.fields, earlier.fields ?? {}, where, `${path}${name}.`));
		} else if (rule.type === "array" && rule.items?.fields) {
			errors.push(...checkCompletable(rule.items.fields, earlier.items?.fields ?? {}, where, `${path}${name}[].`));
		}
	}
	return errors;
}

// Computes the CONTENT fingerprint of a channel's wire contract: the versions
// of every shared contract (schemas, revisions, upcasters) + the transit
// authorization lists. Only semantic data enters the hash — examples and
// counterexamples don't (two catalogs differing only there are wire-compatible),
// and neither do source formatting or declaration order (canonical
// serialization: keys sorted recursively, authorization lists sorted), so a
// non-JS side recomputing the hash from its own copy of the catalog lands on
// the same string. This hash guards against ACCIDENTAL drift (a stale client,
// a forgotten manual millésime) — it is not a defense against a lying client;
// that is what payload validation is for.
export function fingerprintOf(shared, clientEmits, clientReceives) {
	const byDeclaration = (a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : a.version - b.version);
	const semantic = {
		contracts: Object.fromEntries(Object.entries(shared).map(([name, contract]) => [name, contract.versions])),
		clientEmits: [...clientEmits].sort(byDeclaration),
		clientReceives: [...clientReceives].sort(byDeclaration),
	};
	return fnv1a64(canonicalStringify(semantic));
}

// JSON with object keys sorted recursively: two identical data structures give
// the same string whatever their declaration order. Exported so a channel can
// compare two revision schemas for equality (and key a verdict cache) with the
// same canonicalization the content fingerprint uses.
export function canonicalStringify(value) {
	if (Array.isArray(value)) return "[" + value.map(canonicalStringify).join(",") + "]";
	if (TYPE_CHECKS.object(value)) {
		return "{" + Object.keys(value).sort().map((k) => JSON.stringify(k) + ":" + canonicalStringify(value[k])).join(",") + "}";
	}
	return JSON.stringify(value);
}

// FNV-1a 64 bits, pure JS (synchronous, no crypto dependency): enough to catch
// accidents, which is the threat model here.
function fnv1a64(text) {
	let hash = 0xcbf29ce484222325n;
	for (let i = 0; i < text.length; i++) {
		hash ^= BigInt(text.charCodeAt(i));
		hash = (hash * 0x100000001b3n) & 0xffffffffffffffffn;
	}
	return hash.toString(16).padStart(16, "0");
}

// The current revision is not stored in the catalog: it is, by definition,
// the highest revision number (§6.4).
export function latestRevision(versionSchema) {
	return Math.max(...Object.keys(versionSchema.revisions).map(Number));
}

// Validates `payload` against the schema of the revision the emitter declared.
// Returns an array of error strings (empty = valid). Catalog defects are also
// reported as errors so a broker can log and drop instead of crashing.
export function validate(versionSchema, emittedRevision, payload) {
	const revision = versionSchema.revisions[emittedRevision];
	if (revision === undefined) return [`revision ${emittedRevision} not in catalog`];
	return validateObject(revision, payload, "");
}

// schema = { fields, oneOf? } — used for the revision root and for nested objects.
function validateObject(schema, value, path) {
	const where = path || "payload";
	if (!TYPE_CHECKS.object(value)) return [`${where}: object expected`];
	if (!TYPE_CHECKS.object(schema.fields)) return [`${where}: catalog defect — object rule without "fields" (R4)`];
	const errors = [];
	for (const key of Object.keys(value)) {
		if (!(key in schema.fields)) errors.push(`${path}${key}: field not declared in catalog (R1)`);
	}
	if (schema.oneOf) {
		const present = schema.oneOf.filter((k) => value[k] !== undefined);
		if (present.length !== 1) {
			errors.push(`${where}: exactly one of [${schema.oneOf.join(", ")}] expected, got ${present.length}`);
		}
	}
	for (const [name, rule] of Object.entries(schema.fields)) {
		const v = value[name];
		if (v === undefined) {
			if (rule.required) errors.push(`${path}${name}: required`);
			continue;
		}
		errors.push(...validateValue(rule, v, path + name));
	}
	return errors;
}

function validateValue(rule, value, path) {
	const check = TYPE_CHECKS[rule.type];
	if (!check) return [`${path}: catalog defect — unknown type "${rule.type}"`];
	if (!check(value)) return [`${path}: ${rule.type} expected`];
	const errors = [];
	if (rule.pattern && !new RegExp(rule.pattern).test(value)) errors.push(`${path}: pattern mismatch`);
	if (rule.enum && !rule.enum.includes(value)) errors.push(`${path}: value not in [${rule.enum.join(", ")}]`);
	if (rule.type === "object") errors.push(...validateObject(rule, value, path + "."));
	if (rule.type === "array") {
		if (rule.items === undefined) return [`${path}: catalog defect — array rule without "items" (R4)`];
		value.forEach((el, i) => errors.push(...validateValue(rule.items, el, `${path}[${i}]`)));
	}
	return errors;
}

// Converts a payload validated against `emittedRevision` into the canonical form
// (latest revision) delivered to every subscriber (§6.4). Assumes a payload that
// already passed validate(). Returns a new payload; the input is never mutated.
export function upcastToLatestRevision(versionSchema, emittedRevision, payload) {
	const emitted = versionSchema.revisions[emittedRevision];
	if (emitted === undefined) throw new Error(`revision ${emittedRevision} not in catalog`);
	const latest = versionSchema.revisions[latestRevision(versionSchema)];
	const result = structuredClone(payload);
	if (emitted !== latest) upcastFields(latest.fields, emitted.fields, result);
	return result;
}

// Fills defaults recursively, in place. A field gets its default when the latest
// revision promises it (required + default) but the emitter's revision could
// legitimately omit it: unknown to that revision, or known but not yet required
// (R5 allows required to go false -> true). Never creates an absent block —
// only the inside of blocks the emitter actually sent is completed.
function upcastFields(currentFields, emittedFields, payload) {
	for (const [name, rule] of Object.entries(currentFields)) {
		const emittedRule = emittedFields[name];
		const v = payload[name];
		if (v === undefined) {
			const omittable = emittedRule === undefined || !emittedRule.required;
			if (rule.required && omittable && rule.default !== undefined) {
				payload[name] = structuredClone(rule.default);
			}
			continue;
		}
		if (emittedRule === undefined) continue; // value present but new to this revision: validate() already rejected it
		if (rule.type === "object" && rule.fields && TYPE_CHECKS.object(v)) {
			upcastFields(rule.fields, emittedRule.fields ?? {}, v);
		} else if (rule.type === "array" && rule.items?.fields && Array.isArray(v)) {
			for (const el of v) {
				if (TYPE_CHECKS.object(el)) upcastFields(rule.items.fields, emittedRule.items?.fields ?? {}, el);
			}
		}
	}
}

// Completes a payload whose revision is unknown — typically the output of a
// version upcaster (§2.2.d), written when the target version had fewer
// revisions — into the canonical form: every field the latest revision
// requires with a default, missing from the payload, gets that default,
// recursively inside the blocks the payload actually carries. An upcaster's
// "defaults" thus never needs to duplicate revision defaults. Returns a new
// payload; the input is never mutated.
export function completeToLatestRevision(versionSchema, payload) {
	const result = structuredClone(payload);
	completeFields(versionSchema.revisions[latestRevision(versionSchema)].fields, result);
	return result;
}

function completeFields(fields, payload) {
	for (const [name, rule] of Object.entries(fields)) {
		const v = payload[name];
		if (v === undefined) {
			if (rule.required && rule.default !== undefined) payload[name] = structuredClone(rule.default);
			continue;
		}
		if (rule.type === "object" && rule.fields && TYPE_CHECKS.object(v)) {
			completeFields(rule.fields, v);
		} else if (rule.type === "array" && rule.items?.fields && TYPE_CHECKS.array(v)) {
			for (const el of v) {
				if (TYPE_CHECKS.object(el)) completeFields(rule.items.fields, el);
			}
		}
	}
}

// Interprets a declarative version upcaster (breaking change vN-1 -> vN, §2.2.d):
// { rename: { "old.path": "new.path" }, defaults: { "path": value }, remove: ["path"] }.
// Paths are dot-separated to reach inside blocks. Returns a new payload.
export function upcast(upcastRule, payload) {
	const result = structuredClone(payload);
	for (const [from, to] of Object.entries(upcastRule.rename ?? {})) {
		const v = getPath(result, from);
		if (v !== undefined) {
			deletePath(result, from);
			setPath(result, to, v);
		}
	}
	for (const [path, value] of Object.entries(upcastRule.defaults ?? {})) {
		if (getPath(result, path) === undefined) setPath(result, path, structuredClone(value));
	}
	for (const path of upcastRule.remove ?? []) deletePath(result, path);
	return result;
}

function getPath(obj, path) {
	return path.split(".").reduce((o, k) => (TYPE_CHECKS.object(o) ? o[k] : undefined), obj);
}

function setPath(obj, path, value) {
	const keys = path.split(".");
	const last = keys.pop();
	let o = obj;
	for (const k of keys) {
		if (!TYPE_CHECKS.object(o[k])) o[k] = {};
		o = o[k];
	}
	o[last] = value;
}

function deletePath(obj, path) {
	const keys = path.split(".");
	const last = keys.pop();
	const parent = keys.length ? getPath(obj, keys.join(".")) : obj;
	if (TYPE_CHECKS.object(parent)) delete parent[last];
}
