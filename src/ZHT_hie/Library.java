import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

//This class include most of the global variables. The init method is also only fill those variables.

public class Library {
	public static long netBandWidth;// b/s
	public static long latency; //in us, network latency
	public static long msgSize;
	public static long commuOverhead; // single request latency 
	public static long sendOverhead; //packing overhead
	public static long recvOverhead;// unpacking

	public static long procTime; //process a msg time

	public static long numOperaFinished;
	public static long numAllMessage;
	public static long numAllOpera;

	public static long taskId;

	public static void initLib(int numOpera) {
		Library.netBandWidth = 6800000000L; // 6.8Gb/s for bgp
		Library.latency = 5L; //in us
		if (Network.size() >= 1) {
			Library.latency = Library.latency
					* (long) (Math.log10(Network.size()) / Math.log10(2))
					* (long) (Math.sqrt(Network.size() / 1024) + 1.5);
		}
		Library.msgSize = 134L;
		Library.commuOverhead = Library.msgSize * 8L * 1000000L
				/ Library.netBandWidth + Library.latency;
		System.out.println(Library.commuOverhead);
		Library.sendOverhead = Library.recvOverhead = 3L;

		Library.procTime = 400L;

		Library.numOperaFinished = 0;

		Library.numAllMessage = 0;

		Library.numAllOpera = (long) Network.size() * (long) numOpera;

		Library.taskId = 0;
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
