import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import static java.lang.Math.pow;
//This class include most of the global variables. The init method is also only fill those variables.

public class Library {

	private static final String PARA_netBandWidth = "netBandWidth";
	private static final String PARA_latency = "latency";
	private static final String PARA_msgSize = "msgSize";
	//private static final String PARA_commuOverhead = "commuOverhead";
	private static final String PARA_sendOverhead = "sendOverhead";
	private static final String PARA_recvOverhead = "recvOverhead";
	private static final String PARA_procTime = "procTime";
	private static final String PARA_proxyRate = "proxyRate";
	private static final String PAR_BRANCH = "branch";
	
	private static String prefix;

	public static long netBandWidth;// b/s
	public static long latency; // in us, network latency
	public static long msgSize;
	public static long commuOverhead; // single request latency
	public static long sendOverhead; // packing overhead
	public static long recvOverhead;// unpacking

	public static long procTime; // process a msg time on local place

	public static long numOperaFinished;
	public static long numAllMessage;
	public static long numAllOpera;

	public static long taskId;
	public static int proxyRate;
	public static int branch;
	
	public static void initLib(String prefix_t, int numOpera) {
		Library.prefix = prefix_t;

		Library.netBandWidth = initFromConfig(PARA_netBandWidth); 
		// 6800000000L, 6.8Gb/s, for bgp;
		Library.latency = initFromConfig(PARA_latency);// ;5L; //in us
		if (Network.size() >= 1) {
			Library.latency = Library.latency
					* (long) (Math.log10(Network.size()) / Math.log10(2))
					* (long) (Math.sqrt(Network.size() / 1024) + 1.5);
		}
		Library.msgSize = initFromConfig(PARA_msgSize); // 134L;
		Library.commuOverhead = Library.msgSize * 8L * 1000000L
				/ Library.netBandWidth + Library.latency;
		//System.out.println(Library.commuOverhead);
		Library.sendOverhead = initFromConfig(PARA_sendOverhead);
				
		Library.recvOverhead = initFromConfig(PARA_recvOverhead);// 3L;

		Library.procTime = initFromConfig(PARA_procTime);// 400L;

		Library.numOperaFinished = 0;

		Library.numAllMessage = 0;

		Library.numAllOpera = (long) Network.size() * (long) numOpera;

		Library.taskId = 0;
		
		Library.proxyRate = Configuration.getInt(prefix + "." + PARA_proxyRate);
		Library.branch = Configuration.getInt(prefix + "." + PAR_BRANCH);
		
		if(1 == branch){// not used
			/*
			double f = Math.log(Network.size());//Math.pow(Network.size(), 0.00001) ;
			double proxyFactor = 10 * proxyRate * f;//proxyRate * 1; 
			double x = 10 * Math.pow(proxyFactor, 0.0001);
			Library.commuOverhead *= 10* Math.sqrt(x);
			Library.sendOverhead *= x;
			Library.recvOverhead *= x;
			*/
			
			//Library.procTime *= Math.sqrt(proxyRate);
			//Library.procTime *= proxyFactor;
		}		
	}

	private static long initFromConfig(String paraName) {
		return Configuration.getInt(Library.prefix + "." + paraName);
	}

	/* calculate the throughput of each server */
	public static void calServThroughput(int pid) {
		for (int i = 0; i < Network.size(); i++) {
			Node node = Network.get(i);
			PeerProtocol pp = (PeerProtocol) node.getProtocol(pid);
			pp.throughput = (double) pp.numReqRecv
					/ (double) (CommonState.getTime() + Library.recvOverhead)
					* 1E6;
		}
	}
}
