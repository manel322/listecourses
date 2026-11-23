// public/components/echo_panel.js — composant client d'exemple : un champ, émet ping, affiche pong
class EchoPanel {

	instanceName;
	broker;
	deps;
	supports;
	support;
	verbose = false;

	emits    = [{ name: "ping", version: 1 }];
	receives = [{ name: "pong", version: 1 }];

	constructor(broker, instanceName, deps = {}) {
		this.broker = broker;
		this.instanceName = instanceName;
		this.deps = deps;
		this.supports = deps.support?.nodeType ? { main: deps.support } : (deps.support ?? {});
		this.support  = this.supports.main;
		this.render();
		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): register");
		this.broker.register(this, this.emits, this.receives);
	}

	render() {
		this.support.innerHTML = `<input placeholder="un texte"> <button>ping</button> <output></output>`;
		this.support.querySelector("button").onclick = () => {
			this.broker.publish(this, { ping: { texte: this.support.querySelector("input").value } });
		};
	}

	handleMessage(message) {
		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): reçoit " + JSON.stringify(message));
		if (message.pong !== undefined) this.support.querySelector("output").textContent = message.pong.texte;
	}

	destroy() {
		this.broker.unregister(this, this.emits, this.receives);
		for (const node of Object.values(this.supports)) node.replaceChildren();
	}

}
export default EchoPanel;
