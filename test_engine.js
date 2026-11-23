/*
 * test_engine.js — rejoue les examples / counterexamples du catalogue.
 *   node test_engine.js
 * Chaque exemple doit valider sa révision ; chaque contre-exemple doit être rejeté.
 */
import { validate, validateCatalog } from "./fw/engine.js";
import CATALOG from "./catalogs/catalog.js";

let echecs = 0;
for (const defaut of validateCatalog(CATALOG)) { console.error("  catalogue : " + defaut); echecs++; }

for (const [nom, contrat] of Object.entries(CATALOG)) {
	for (const [v, parVersion] of Object.entries(contrat.examples ?? {})) {
		for (const [r, exemples] of Object.entries(parVersion)) {
			for (const payload of exemples) {
				const erreurs = validate(contrat.versions[v], Number(r), payload);
				if (erreurs.length > 0) { console.error(`  ${nom} v${v} r${r} : exemple rejeté — ${erreurs.join(", ")}`); echecs++; }
			}
		}
	}
	for (const [v, parVersion] of Object.entries(contrat.counterexamples ?? {})) {
		for (const [r, contres] of Object.entries(parVersion)) {
			for (const { reason, payload } of contres) {
				const erreurs = validate(contrat.versions[v], Number(r), payload);
				if (erreurs.length === 0) { console.error(`  ${nom} v${v} r${r} : contre-exemple accepté (${reason})`); echecs++; }
			}
		}
	}
}
console.log(echecs === 0 ? "test_engine : catalogue prouvé" : `test_engine : ${echecs} échec(s)`);
process.exit(echecs === 0 ? 0 : 1);
