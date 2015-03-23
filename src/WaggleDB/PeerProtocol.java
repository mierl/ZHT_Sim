/* The protocol of peer (both server and client), it implements all the 
 * behavior of the peer
 */

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;

import java.math.*;
import java.util.*;

public class PeerProtocol implements EDProtocol {
	/*
	 * static configuration parameters read from the configuration file
	 * PARA_TRANSPORT: specify the transport layer protocol PARA_NUMSERVER:
	 * specify the number of servers PARA_NUMOPERATION: specify the number of
	 * operations per client PARA_IDLENGTH: specify the length of key space
	 * PARA_GATHERSIZE: for ctree, specify the threshold for gathering requests
	 * PARA_NUMREPLICA: specify the number of replicas
	 */
	private static final String PARA_TRANSPORT = "transport";
	private static final String PARA_IDLENGTH = "idLength";

	/*
	 * values of parameters read from the configuration file, corresponding to
	 * configuration parameters par: includes transport layer protocol id, and
	 * peer protocol id numServer: number of servers numOpera: number of
	 * operations per server idLength: the length of key space, from 0 to
	 * 2^idLength - 1 gatherSize: the threshold for gathering requests for ctree
	 * numReplica: the number of replicas
	 */
	public Parameters par;
	public int numClient;
	public int numServer;
	public int numOpera;
	public int idLength;

	/*
	 * attributes of a peer prefix: the prefix of configuration file id: the
	 * global id of a peer servId: for a client, which server it connects to
	 * maxTime: for a server, the occurrence time of the last operation. for the
	 * purpose of serialization of all the request
	 */
	public String prefix;
	public BigInteger id;
	public long maxTime;
	public long maxFwdTime;

	/*
	 * for dfc churn memList: the membership list first: whether it is a
	 * broadcast initializer or receiver
	 */

	/*
	 * for dchord predecessor: the predecessor of a server fingerTable: the
	 * finger table in which all the nodes the server could talk directly
	 * succListSize: the size of successor list succList: the successors
	 * messCount: for distinguishing the messages in message routing
	 */
	public long msgCount;

	/* Hash Map for storing the (key, value) data */
	public HashMap<String, String> hmData;

	/* Counters for statistics */
	public long numReqRecv;

	public long numAllReqFinished;

	public long oldnumAllReqFinished;
	public double throughput;

	public double clientThroughput;

	public double[] latencyStat;

	public int lastServerId;

	/*
	 * initialization read the parameters from the configuration file
	 */
	public PeerProtocol(String prefix) {
		this.prefix = prefix;
		this.par = new Parameters();
		this.par.tid = Configuration.getPid(prefix + "." + PARA_TRANSPORT);
		this.idLength = Configuration.getInt(prefix + "." + PARA_IDLENGTH);
	}

	public void operaMsgProcess(OperaMessage om, Node node) {
		numReqRecv++;
		updateMaxFwdTime(Library.recvOverhead);
		Library.taskHM.get(om.taskId).taskQueuedTime = maxFwdTime;
		ResClientMessage rcm = (ResClientMessage) actOperaMsgProcess(om, node);
		Library.taskHM.get(om.taskId).taskEndTime = maxTime;
		procToFwd();
		updateMaxFwdTime(Library.sendOverhead);
		EDSimulator.add(waitTimeCal(maxFwdTime), rcm, om.sender, par.pid);
	}

	public void logEvent(Node node, int pid) {
		Double[] throughputArray = new Double[numServer];
		for (int i = 0; i < numServer; i++) {
			Node svrNode = Network.get(i + numClient);// server only
			PeerProtocol pp = (PeerProtocol) (svrNode.getProtocol(par.pid));
			pp.throughput = (double) (pp.numAllReqFinished - pp.oldnumAllReqFinished)
					/ Library.logInterval * 1E6;
			throughputArray[i] = pp.throughput;
			pp.oldnumAllReqFinished = pp.numAllReqFinished;
		}
		Library.throughputLogAL.add(throughputArray);
		if (Library.numOperaFinished < Library.numAllOpera) {
			String logEventStr = "ThroughputLog";
			long timeDiff = Library.logInterval;
			EDSimulator.add(timeDiff, logEventStr, Network.get(0), pid);
		}
	}

	public void processEvent(Node node, int pid, Object event) {
		Library.numAllMessage++;
		if (event.getClass() == OperaMessage.class) {
			OperaMessage om = (OperaMessage) event;
			operaMsgProcess(om, node);
		} else if (event.getClass() == ResClientMessage.class) {
			ResClientMessage rcm = (ResClientMessage) event;
			resClientMsgProcess(node, rcm);
		} else if (event.getClass() == String.class) {
			String eventStr = (String) event;
			if (eventStr.equals("ThroughputLog")) {
				logEvent(node, pid);
			}
		} else {
			System.out.println("Error message type!");
		}
	}

	public Object clone() {
		PeerProtocol pp = new PeerProtocol(this.prefix);
		return pp;
	}

	/* create the task description upon submitting for the purpose of logging */
	public void createTask(Node sender, int type, long wait) {
		Library.taskId++;
		TaskDetail td = new TaskDetail(Library.taskId, id.intValue(), "no",
				"get", CommonState.getTime() + wait, 0, 0, 0, 1);
		Library.taskHM.put(Library.taskId, td);
	}

	public int chooseServer() {
		int serverIdx = -1;
		double minLatency = latencyStat[0];
		for (int i = 1; i < numServer; i++) {
			if (minLatency < latencyStat[i])
				minLatency = latencyStat[i];
		}
		ArrayList<Integer> al = new ArrayList<Integer>();
		for (int i = 0; i < numServer; i++) {
			if (minLatency == latencyStat[i])
				al.add(i);
		}
		serverIdx = al.get(CommonState.r.nextInt(al.size())) + numClient;// CommonState.r
																			// random.
		lastServerId = serverIdx - numClient;
		return serverIdx;
	}

	/* submit a get request */
	public void doGet(Node sender, int pid, long wait) {
		BigInteger ranKey = new BigInteger(idLength, CommonState.r);
		String key = ranKey.toString();
		OperaMessage om = new OperaMessage(0, sender, 0, key, null);
		long time = wait;
		int destId = chooseServer();
		time += Library.sendOverhead + Library.commOverhead;
		createTask(sender, 0, wait);
		om.taskId = Library.taskId;
		EDSimulator.add(time, om, Network.get(destId), pid);
	}

	/* submit a request */
	public void doRequest(Node sender, int pid, long wait) {
		// double ran = CommonState.r.nextDouble();
		// if (ran >= 0.5) {
		doGet(sender, pid, wait);
		// } else {
		// doPut(sender, idLength, pid, wait);
		// }
	}

	/* update max time */
	public void updateMaxTime(long increment) {
		if (CommonState.getTime() > maxTime) {
			maxTime = CommonState.getTime();
		}
		maxTime += increment;
	}

	public void updateMaxFwdTime(long increment) {
		if (CommonState.getTime() > maxFwdTime) {
			maxFwdTime = CommonState.getTime();
		}
		maxFwdTime += increment;
	}

	public void procToFwd() {
		if (maxFwdTime < maxTime) {
			maxFwdTime = maxTime;
		}
	}

	public void fwdToProc() {
		if (maxTime < maxFwdTime) {
			maxTime = maxFwdTime;
		}
	}

	public long waitTimeCal(long endTime) {
		return endTime - CommonState.getTime() + Library.commOverhead;
	}

	/* Do actual operation */
	public Object actOperaMsgProcess(OperaMessage om, Node node) {
		String value = null;
		ResClientMessage rcm = null;
		value = hmData.get(om.key);
		fwdToProc();
		updateMaxTime(Library.procTime);
		rcm = new ResClientMessage(om.taskId, om.type, om.key, value, true,
				true, 0, id.intValue());
		return rcm;
	}

	/* response to client message processing */
	public void resClientMsgProcess(Node node, ResClientMessage rcm) {
		Node svrNode = Network.get(rcm.sourceNodeId);
		PeerProtocol pp = (PeerProtocol) svrNode.getProtocol(par.pid);
		pp.numAllReqFinished++;
		numAllReqFinished++;
		Library.numOperaFinished++;
		Library.taskHM.get(rcm.taskId).taskBackClientTime = CommonState
				.getTime();
		TaskDetail td = Library.taskHM.get(rcm.taskId);
		long taskLatency = td.taskBackClientTime - td.taskSubmitTime;
		latencyStat[lastServerId] = (latencyStat[lastServerId] + taskLatency) / 2; // average
																					// latency
																					// calculation
		if (numAllReqFinished < numOpera) {
			doRequest(node, par.pid, Library.recvOverhead);
		} else {
			clientThroughput = (double) numOpera
					/ (double) (CommonState.getTime() + Library.recvOverhead);
		}
		if (Library.numOperaFinished == Library.numAllOpera) {
			System.out.println("The simulation time is:"
					+ (CommonState.getTime() + Library.recvOverhead));
			System.out.println("The throughput is:"
					+ (double) (Library.numAllOpera)
					/ (double) (CommonState.getTime() + Library.recvOverhead)
					* 1E6);
			Library.logServer(par.pid);
			Library.logTask();
			Library.logRealTimeThroughput();//write to file
		}
	}
}