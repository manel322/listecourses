// tests/probe.js — la sonde : un objet standard minimal qui garde tout ce qu'il reçoit
export class Probe {
	instanceName;
	received = [];
	constructor(instanceName) { this.instanceName = instanceName; }
	handleMessage(message) { this.received.push(message); }
}
