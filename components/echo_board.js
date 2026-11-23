// components/echo_board.js — composant serveur d'exemple : répond pong à ping, ciblé sur le demandeur
class EchoBoard {

	instanceName;
	broker;
	deps;
	verbose = false;

	emits    = [{ name: "pong", version: 1 }];
	receives = [{ name: "ping", version: 1 }];

	constructor(broker, instanceName, deps = {}) {
		this.broker = broker;
		this.instanceName = instanceName;
		this.deps = deps;
		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): register");
		this.broker.register(this, this.emits, this.receives);
	}

	handleMessage(message) {
		if (this.broker.verbose || this.verbose) console.log("INFO (" + this.instanceName + "): reçoit " + JSON.stringify(message));
		if (message.ping !== undefined) {
			this.broker.publish(this, { pong: { texte: message.ping.texte.toUpperCase() }, id: message.id });
		}
	}

	destroy() {
		this.broker.unregister(this, this.emits, this.receives);
	}

}
export default EchoBoard;
