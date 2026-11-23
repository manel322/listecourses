// catalogs/catalog.js — union du broker SERVEUR : framework + canaux + contrats serveur-only
import { CHANNEL_CONTRACTS } from "../fw/channel.catalog.js";
import { SHARED as APP } from "../public/catalogs/app.catalog.js";

const LOCAL = {}; // contrats serveur-only, s'il y en a

export const CATALOG = { ...CHANNEL_CONTRACTS, ...APP, ...LOCAL };
export default CATALOG;
