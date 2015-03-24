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
	public int numReplica;

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
	
	public long localTransTime;
	/* Hash Map for storing the (key, value) data */
	public HashMap<String, String> hmData;


	/* Counters for statistics */
	public long numReqRecv;

	public long numAllReqFinished;

	public double throughput;
	public double clientThroughput;
	
	public long reqArrivalInterval;
	public int numReqSubmitted;
	public int batchSize;
	public long timeThreshold;
	public ArrayList<OperaMessage>[] batchVector;

	/*
	 * initialization read the parameters from the configuration file
	 */
	public PeerProtocol(String prefix) {
		this.prefix = prefix;
		this.par = new Parameters();
		this.par.tid = Configuration.getPid(prefix + "." + PARA_TRANSPORT);
		this.idLength = Configuration.getInt(prefix + "." + PARA_IDLENGTH);
	}

	public Object operaMsgProcess(OperaMessage om, Node node) {
		numReqRecv++;
		ResClientMessage rcm = (ResClientMessage) actOperaMsgProcess(om,node);
		return rcm;
//		procToFwd();
//			updateMaxFwdTime(Library.sendOverhead);
//			if (node.getIndex() != om.sender.getIndex()) {
//				EDSimulator.add(waitTimeCal(maxFwdTime), rcm, om.sender,
//						par.pid);
//			} else {
//				EDSimulator.add(maxFwdTime - CommonState.getTime(), rcm,
//						om.sender, par.pid);
//			}
	}
	
	public void procBatchMsg(BatchMessage bm, Node node) {
		updateMaxFwdTime(Library.recvOverhead);
		for (int i = 0; i < bm.requests.length; i++) {
			Library.taskHM.get(bm.requests[i].taskId).taskQueuedTime = maxFwdTime;
		}
		BatchResClientMsg batchResClientMsg = new BatchResClientMsg();
		batchResClientMsg.batchRetMsg = new ArrayList<ResClientMessage>();
		for (int i = 0; i < bm.requests.length; i++) {
			batchResClientMsg.batchRetMsg.add((
					ResClientMessage)operaMsgProcess(bm.requests[i], node));
		}
		procToFwd();
		updateMaxFwdTime(Library.sendOverhead);
		long time = maxFwdTime + Library.msgSize * 8L * 1000000L * (long)bm.requests.length
				/ Library.netSpeed + Library.latency;
		EDSimulator.add(time - CommonState.getTime(), 
				batchResClientMsg, bm.requests[0].sender, par.pid);
	}
	
	/* response to client message processing */
	public void resClientMsgProcess(Node node, ResClientMessage rcm) {
		numAllReqFinished++;
		Library.numOperaFinished++;
		if (numAllReqFinished < numOpera) {
			doRequest(node, par.pid, Library.recvOverhead);
		} else {
			clientThroughput = (double) numOpera
					/ (double) (CommonState.getTime() + Library.recvOverhead);
		}
		if (Library.numOperaFinished == Library.numAllOpera) {
			System.out.println("The simulation time is:"
					+ (CommonState.getTime() + Library.recvOverhead));
			double th = (double) (Library.numAllOpera)
					/ (double) (CommonState.getTime() + Library.recvOverhead)
					* 1E6;
			System.out.println("The throughput is: " + th);
			//System.out.println("Single node throughput is : " + th/(numClient));
			
		}
	}
	
	public void procBatchRetMsg(BatchResClientMsg brm, Node node) {
		numAllReqFinished += brm.batchRetMsg.size();
		Library.numOperaFinished += brm.batchRetMsg.size();
		for (int i = 0; i < brm.batchRetMsg.size(); i++)
			Library.taskHM.get(brm.batchRetMsg.get(i).taskId).
			taskBackClientTime = CommonState.getTime();
		if (numAllReqFinished == numOpera)
			clientThroughput = (double) numOpera
			/ (double) (CommonState.getTime() + Library.recvOverhead);
		if (Library.numOperaFinished == Library.numAllOpera) {
			System.out.println("The simulation time is:"
					+ (CommonState.getTime() + Library.recvOverhead));
			double th = (double) (Library.numAllOpera)
					/ (double) (CommonState.getTime() + Library.recvOverhead)
					* 1E6;
			System.out.println("The throughput is: " + th + " on " + numServer + " nodes.");
			System.out.println("Single node throughput is : " + th/(numClient));
			Library.logServer(par.pid);
			Library.logTask();
		}
	}
	
	public void processEvent(Node node, int pid, Object event) {
		Library.numAllMessage++;
		if (event.getClass() == BatchMessage.class) {
			BatchMessage bm = (BatchMessage) event;
			procBatchMsg(bm, node);
		} else if (event.getClass() == BatchResClientMsg.class) {
			BatchResClientMsg brm = (BatchResClientMsg) event;
			procBatchRetMsg(brm, node);
//		} else if (event.getClass() == ReplicaMessage.class) {
//			ReplicaMessage rm = (ReplicaMessage) event;
//			replicaMsgProcess(node, rm);
//		} else if (event.getClass() == ResReplicaMessage.class) {
//			ResReplicaMessage rrm = (ResReplicaMessage) event;
//			resReplicaMsgProcess(node, rrm);
		} else if (event.getClass() == String.class) {
			doRequest(node, pid, 0);
		}
	}

	public Object clone() {
		PeerProtocol pp = new PeerProtocol(this.prefix);
		return pp;
	}

	/* create the task description upon submitting for the purpose of logging */
	public void createTask(Node sender, int type, long wait) {
		Library.taskId++;
		TaskDetail td = new TaskDetail(Library.taskId, id.intValue(), 
				"no", "get", CommonState.getTime() + wait, 0, 0, 0, 0, 1);
		Library.taskHM.put(Library.taskId, td);
	}
	
	public void batchSend(Node sender, int pid, long time, int destId, int size) {
		time += localTransTime * (long)size;
		for (int i = 0; i < size; i++) {
			Library.taskHM.get(batchVector[destId].get(i).taskId).
				taskSentTime = CommonState.getTime() + time;
		}
		time += Library.sendOverhead + Library.msgSize * 8L * 1000000L * (long)size
				/ Library.netSpeed + Library.latency;
		BatchMessage bm = new BatchMessage();
		bm.sender = sender;
		bm.requests = new OperaMessage[batchVector[destId].size()];
		batchVector[destId].toArray(bm.requests);
		batchVector[destId].clear();
		EDSimulator.add(time, bm, Network.get(destId + numClient), pid);
	}
	
//	/* submit a get request */
//	public void doGet(Node sender, int pid, long wait) {
//		BigInteger ranKey = new BigInteger(idLength, CommonState.r);
//		String key = ranKey.toString();
//		long qos =0;
//		OperaMessage om = new OperaMessage(0, sender, 0, key, null, qos);
//		createTask(sender, 0, wait);
//		om.taskId = Library.taskId;
//		long time = wait;
//		int destId = hashServer(new BigInteger(key), numServer);
//		batchVector[destId].add(om);
//		if (Library.batchPolicy.equals("BatchSize") && batchVector[destId].size() == batchSize) {
//			
//			batchSend(sender, pid, time, destId, batchSize);
//		} else { // other policies
//			
//		}
//	}
//
//	/* submit a put request */
//	public void doPut(Node sender, int pid, long wait) {
//		BigInteger ranKey = new BigInteger(idLength, CommonState.r);
//		String key = ranKey.toString();
//		byte[] valueByte = new byte[134];
//		CommonState.r.nextBytes(valueByte);
//		String value = valueByte.toString();
//		long qos =0;
//		OperaMessage om = new OperaMessage(0, sender, 1, key, value, qos);
//		createTask(sender, 1, wait);
//		om.taskId = Library.taskId;
//		long time = wait;
//		int destId = hashServer(new BigInteger(key), numServer);
//		batchVector[destId].add(om);
//		if (Library.batchPolicy.equals("BatchSize") && batchVector[destId].size() == batchSize) {
//			batchSend(sender, pid, time, destId, batchSize);
//		} else { // other policies
//			
//		}
//	}
	
	public void doBatchRequest(Node sender, int pid, long wait, int isPut){
		//Generate 
		BigInteger ranKey = new BigInteger(idLength, CommonState.r);
		String key = ranKey.toString();
		String value;
		if(1==isPut){
			byte[] valueByte = new byte[20];
			CommonState.r.nextBytes(valueByte);
			value = valueByte.toString();
		}else{
			value = null;
		}
		
		long qos =0;
		
		OperaMessage om = new OperaMessage(0, sender, isPut, key, value, qos);
		createTask(sender, 1, wait);
		om.taskId = Library.taskId;
		long time = wait;
		int destId = hashServer(new BigInteger(key), numServer);
		batchVector[destId].add(om);    //.equals("fixedSize")
		
		if (Library.batchPolicy.equals("fixedSize") && batchVector[destId].size() == batchSize) {
			System.out.println("batchPolicy = " + Library.batchPolicy+ ", batchSize = " + batchSize);
			batchSend(sender, pid, time, destId, batchSize);
		} else { // other policies
			System.out.println("BatchPolicy = " + Library.batchPolicy + ", batchSize = " + batchSize);
		}

	}
	
	/* submit a request */
	public void doRequest(Node sender, int pid, long wait) {
		double ran = CommonState.r.nextDouble();
		
		
		
		if (ran >= 0.5) {
			//doGet(sender, pid, wait);
			doBatchRequest(sender, pid, wait, 0);
		} else {
			//doPut(sender, pid, wait);
			doBatchRequest(sender, pid, wait, 1);
		}
		
		
		numReqSubmitted++;
		if (numReqSubmitted < numOpera) {
			String str = "submitTask";
			long time = wait + reqArrivalInterval;
			EDSimulator.add(time, str, sender, pid);
		} else {
			for (int i = 0; i < numServer; i++) {
				if (batchVector[i].size() > 0) {
					batchSend(sender, pid, wait, i, batchVector[i].size());
				}
			}
		}
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

	/* update the times of task */
	public void updateTask(TaskDetail td) {
		td.taskQueuedTime = CommonState.getTime();
		td.taskEndTime = maxTime;
	}

	/* hash to the correct server */
	public int hashServer(BigInteger key, int numServer) {
		return Integer.parseInt(key.mod(BigInteger.valueOf((long) numServer))
				.toString());
	}


	/* Do actual operation */
	public Object actOperaMsgProcess(OperaMessage om, Node node) {
		String value = null;
		ResClientMessage rcm = null;
		if (om.type == 0) {
			value = hmData.get(om.key);
		} else {
			value = om.value;
			hmData.put(om.key, om.value);
		}
		fwdToProc();
		updateMaxTime(Library.procTime);
		Library.taskHM.get(om.taskId).taskEndTime = maxTime;
		rcm = new ResClientMessage(om.taskId, om.type, om.key, value, true, true, 0);
		return rcm;
	}


	/* handle the replication message */
//	public void replicaMsgProcess(Node node, ReplicaMessage rm) {
//		ResReplicaMessage rrm = null;
//		hmData.put(rm.key, rm.value);
//		rrm = new ResReplicaMessage(rm.messageId, true);
//		updateMaxFwdTime(Library.recvOverhead);
//		fwdToProc();
//		updateMaxTime(Library.procTime);
//		procToFwd();
//		updateMaxFwdTime(Library.sendOverhead);
//		EDSimulator.add(waitTimeCal(maxFwdTime), rrm, rm.sender, par.pid);
//		numFwdMsg++;
//	}

	/* handle the replication response message */
//	public void resReplicaMsgProcess(Node sender, ResReplicaMessage rrm) {
//		updateMaxFwdTime(Library.recvOverhead);
//		ReplicaInfo ri = hmReplica.get(rrm.messageId);
//		ri.numReplicaRecv++;
//		if (ri.numReplicaRecv != numReplica) {
//			doReplica(sender, ri.om, ri.numReplicaRecv, rrm.messageId, 0);
//		} else {
//			ResClientMessage rcm = new ResClientMessage(ri.om.taskId, 1,
//					ri.om.key, ri.om.value, true, true, 0);
//			hmData.put(ri.om.key, ri.om.value);
//			updateMaxFwdTime(Library.sendOverhead);
//			if (ri.om.sender.getIndex() != sender.getIndex()) {
//				EDSimulator.add(waitTimeCal(maxFwdTime), rcm, ri.om.sender,
//						par.pid);
//			} else {
//				EDSimulator.add(maxFwdTime - CommonState.getTime(), rcm,
//						ri.om.sender, par.pid);
//			}
//			numFwdMsg++;
//		}
//	}
}