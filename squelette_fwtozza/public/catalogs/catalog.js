// public/catalogs/catalog.js — union du broker CLIENT : framework + canaux + contrats client-only
import { CLIENT_CHANNEL_CONTRACTS } from "../fw/channel.catalog.js";
import { SHARED as APP } from "./app.catalog.js";

const LOCAL = {}; // contrats client-only, s'il y en a

export const CATALOG = { ...CLIENT_CHANNEL_CONTRACTS, ...APP, ...LOCAL };
export default CATALOG;
