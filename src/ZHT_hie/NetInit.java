import java.math.BigInteger;
import java.util.*;
import peersim.core.*;
import peersim.config.Configuration;

//Implement interface Control, define the initial state of network
public class NetInit implements Control {
	private static final String PARA_protocol = "protocol";
	private static final String PARA_numOps = "numOperation";
	private static final String PARA_proxyRate = "proxyRate";

	private int pid;
	private int numOpera;
	private String prefix_t;
	private int proxyRate;

	public class InvalidConfigException extends Exception {

		public InvalidConfigException(String message) {
			super(message);
		}

	}

	// Constructor
	public NetInit(String prefix) {
		// PID is given by the PeerSim, so you only use it, not change it.
		// It can be fount by the engine and by given a string name.
		this.pid = Configuration.getPid(prefix + "." + PARA_protocol);
		this.numOpera = Configuration.getInt(prefix + "." + PARA_numOps);
		this.prefix_t = prefix;
		this.proxyRate = Configuration.getInt(prefix + "." + PARA_proxyRate);

	}

	//
	public void netInit() {
		// Node stored in a "array" and can be find with get()
		for (int i = 0; i < Network.size(); i++) {
			Node node = (Node) Network.get(i);
			PeerProtocol pp = (PeerProtocol) node.getProtocol(pid);

			// Assign each node's protocol an ID??? The line below should not do
			// anything.
			// Then this function should not do anything.
			pp.id = new BigInteger(Integer.toString(i));// 人为定义pid为array序号
		}
	}

	public void overallInit() {
		for (int i = 0; i < Network.size(); i++) {
			// 每个node 都有个PeerProtocol??
			Node node = (Node) Network.get(i);
			PeerProtocol pp = (PeerProtocol) node.getProtocol(pid);
			pp.maxProcessQTime = 0;
			pp.maxCommuQTime = 0;
			pp.mapData = new HashMap<String, String>();
			pp.mapReplica = new HashMap<Long, ReplicaInfo>();
			pp.numFwdMsg = 0;
			pp.numAllReqFinished = 0;
			pp.numAllReqFinished = 0;
			pp.throughput = 0;
			pp.numReqRecv = 0;
			pp.clientThroughput = 0;
			// define roles here
			rolesInit(pp, i);

		}
	}

	//没有用上roles变量, 实际上在PP中以逻辑定义了
	public int rolesInit(PeerProtocol pp, int index) {
		if (proxyRate > Network.size()) {
			try {
				throw new InvalidConfigException(
						"\n proxyRate > Network.size, please check config file! \n");
			} catch (InvalidConfigException e) {
				// TODO: proxyRate太大时取第一个node做proxy, 且此操作应该只做一次. 是无关紧要的细节
				e.printStackTrace();
				System.exit(0);
			}
		} else {
			pp.roles.isClient = true;
			pp.roles.isServer = true;
			if (0 == index % proxyRate) {// decide proxy: 用不着roles
				pp.roles.isProxy = true;
			}
		}
		return 0;
	}

	// This is the only method that need to be implemented from Control.
	// Actually Control only has this one method.
	public boolean execute() {
		System.out.println("NetInit start.");

		netInit(); // Tony commet it off: not do anything.
		overallInit(); // This is used.
		Library.initLib(prefix_t, numOpera);
		System.out.println("NetInit end.");

		return false;
	}
}