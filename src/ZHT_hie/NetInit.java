import java.math.BigInteger;
import java.util.*;
import peersim.core.*;
import peersim.config.Configuration;

//Implement interface Control, define the initial state of network
public class NetInit implements Control {
	private static final String PARA_protocol = "protocol";
	private static final String PARA_numOps = "numOperation";

	private int pid;
	private int numOpera;

	// Constructor
	public NetInit(String prefix) {
		// PID is given by the PeerSim, so you only use it, not change it.
		// It can be fount by the engine and by given a string name.
		this.pid = Configuration.getPid(prefix + "." + PARA_protocol);
		this.numOpera = Configuration.getInt(prefix + "." + PARA_numOps);
	}

	//
	public void dfcInit() {
		// Node stored in a "array" and can be find with get()
		for (int i = 0; i < Network.size(); i++) {
			Node node = (Node) Network.get(i);
			PeerProtocol pp = (PeerProtocol) node.getProtocol(pid);
			
			// Assign each node's protocol an ID??? The line below should not do anything. 
			//Then this function should not do anything.
			pp.id = new BigInteger(Integer.toString(i));
		}
	}

	public void overallInit() {
		for (int i = 0; i < Network.size(); i++) {
			Node node = (Node) Network.get(i);
			PeerProtocol pp = (PeerProtocol) node.getProtocol(pid);
			pp.maxTime = 0;
			pp.maxFwdTime = 0;
			pp.mapData = new HashMap<String, String>();
			pp.mapReplica = new HashMap<Long, ReplicaInfo>();
			pp.numFwdMsg = 0;
			pp.numAllReqFinished = 0;
			pp.numAllReqFinished = 0;
			pp.throughput = 0;
			pp.numReqRecv = 0;
			pp.clientThroughput = 0;
		}
	}

	// This is the only method that need to be implemented from Control.
	// Actually Control only has this one method.
	public boolean execute() {
		dfcInit(); //Tony commet it off: not do anything.
		overallInit(); //This is used.
		Library.initLib(numOpera);
		return false;
	}
}