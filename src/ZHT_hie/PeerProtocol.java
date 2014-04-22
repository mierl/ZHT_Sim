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

//Ke, implement.   //all self defined, include class name PeerProtocol
public class PeerProtocol implements EDProtocol {
	/*
	 * static configuration parameters read from the configuration file
	 * PARA_TRANSPORT: specify the transport layer protocol PARA_NUMSERVER:
	 * specify the number of servers PARA_NUMOPERATION: specify the number of
	 * operations per client PARA_IDLENGTH: specify the length of key space
	 * PARA_GATHERSIZE: for ctree, specify the threshold for gathering requests
	 * PARA_NUMREPLICA: specify the number of replicas
	 */

	// follow this way. Read conf file
	private static final String PARA_TRANSPORT = "transport";
	private static final String PARA_NUMSERVER = "numServer";
	private static final String PARA_NUMOPERATION = "numOperation";
	private static final String PARA_IDLENGTH = "idLength";
	private static final String PARA_NUMREPLICA = "numReplica";

	/*
	 * values of parameters read from the configuration file, corresponding to
	 * configuration parameters par: includes transport layer protocol id, and
	 * peer protocol id numServer: number of servers numOpera: number of
	 * operations per server idLength: the length of key space, from 0 to
	 * 2^idLength - 1 gatherSize: the threshold for gathering requests for ctree
	 * numReplica: the number of replicas
	 */

	//
	public Parameters parameters;
	public int numServer;
	public int numOps;
	public int keyLength; //it was named idLength
	public int numReplica;

	/*
	 * attributes of a peer:
	 * prefix: the prefix of configuration file.
	 * id: the global id of a peer servId: for a client, which server it connects to
	 * maxTime: for a server, the occurrence time of the last operation. for the
	 * purpose of serialization of all the request
	 */

	public String prefix; //the prefix of configuration file
	public BigInteger id;//not used???  
	//the global id of a peer servId: for a client, which server it connects to.
	
	public long maxTime;//for a server, the time of last operation. for the purpose of serialization of all the request
	public long maxFwdTime;// ???

	public long count;

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
	public long messCount;

	/* Hash Map for storing the (key, value) data */
	public HashMap<String, String> mapData;

	/* Hash Map for replication */
	public HashMap<Long, ReplicaInfo> mapReplica;

	/* Counters for statistics */
	public long numReqRecv;

	public long numFwdMsg;

	public long numAllReqFinished;

	public double throughput;
	public double clientThroughput;

	/*
	 * initialization read the parameters from the configuration file
	 */

	// constructor of above: read file and assignment/init
	// Only the first obj is create by using this, all others are by clone().
	public PeerProtocol(String prefix) {
		this.prefix = prefix;
		this.parameters = new Parameters();
		this.parameters.tid = Configuration.getPid(prefix + "."
				+ PARA_TRANSPORT);
		this.numServer = Configuration.getInt(prefix + "." + PARA_NUMSERVER);
		this.numOps = Configuration.getInt(prefix + "." + PARA_NUMOPERATION);
		this.keyLength = Configuration.getInt(prefix + "." + PARA_IDLENGTH); 
		this.numReplica = Configuration.getInt(prefix + "." + PARA_NUMREPLICA);
	}

	public Object clone() {
		PeerProtocol pp = new PeerProtocol(this.prefix);
		return pp;
	}

	// System defined, Must implement this function, no change to function name
	// and parameters. Logic body.
	// node: 
	public void processEvent(Node node, int pid, Object event) {

		Library.numAllMessage++;// no much use

		if (event.getClass() == OperationMessage.class) {
			OperationMessage msg = (OperationMessage) event;
			operationMsgProcess(msg, node);

		} else if (event.getClass() == ResClientMessage.class) {
			ResClientMessage msg = (ResClientMessage) event;
			respondClientMsgProcess(node, msg); //executed by client, node: client which recv ack

		} else if (event.getClass() == ReplicaMessage.class) {
			ReplicaMessage msg = (ReplicaMessage) event;
			replicaMsgProcess(node, msg);

		} else if (event.getClass() == ResReplicaMessage.class) {
			ResReplicaMessage msg = (ResReplicaMessage) event;
			resReplicaMsgProcess(node, msg);
		}
	}

	// System defined, Must implement this function, no change to function name
	// and parameters
	// keep this way.

	/* create the task description upon submitting for the purpose of logging */
	// Ke

	/* submit a get request */
	// Ke

	/* update max time */
	// Ke

	/* hash to the correct server */
	public int hashServer(BigInteger key, int numServer) {
		return Integer.parseInt(key.mod(BigInteger.valueOf((long) numServer))
				.toString());
	}

	/* Send messages to other replicas for updating */

	// Ke
	public void operationMsgProcess(OperationMessage opMsg, Node node) {
		numReqRecv++;

		updateMaxFwdTime(Library.recvOverhead);

		if (numReplica == 0 || opMsg.type == 0) { // type 0: get, 1: put
			ResClientMessage responseMsg = (ResClientMessage) actOpMsgProcess(
					opMsg, node);
			procToFwd();
			updateMaxFwdTime(Library.sendOverhead);

			if (node.getIndex() != opMsg.sender.getIndex()) {// ???
				EDSimulator.add(waitTimeCal(maxFwdTime), responseMsg,
						opMsg.sender, parameters.pid);
			} else {
				EDSimulator.add(maxFwdTime - CommonState.getTime(),
						responseMsg, opMsg.sender, parameters.pid);
			}
		} else {
			actOpMsgProcess(opMsg, node);
		}
	}

	/* Do actual operation */
	public Object actOpMsgProcess(OperationMessage opMsg, Node node) {
		String value = null;
		ResClientMessage resMsg = null;
		if (opMsg.type == 0) { // type=0, get
			value = mapData.get(opMsg.key);
		} else if (numReplica == 0) {
			value = opMsg.value;
			mapData.put(opMsg.key, opMsg.value); // type!=0, put
		}
		fwdToProc();
		updateMaxTime(Library.procTime);
		
		if (numReplica > 0 && opMsg.type != 0) {
			messCount++;
			mapReplica.put(messCount, new ReplicaInfo(opMsg, 0));
			doReplica(node, opMsg, 0, messCount, 0);
			return null;
			
		} else {
			resMsg = new ResClientMessage(opMsg.taskId, opMsg.type, opMsg.key,
					value, true, true, 0);
			return resMsg;
		}
	}

	public void doReplica(Node sender, OperationMessage msg, int i,
			long messageId, int time) {
		String key = null, value = null;
		key = msg.key;
		value = msg.value;
		ReplicaMessage repMsg = new ReplicaMessage(sender, messageId, key,
				value);
		int replicaId = (sender.getIndex() + i + 1) % numServer;
		EDSimulator.add(waitTimeCal(maxFwdTime), repMsg,
				Network.get(replicaId), parameters.pid);
		numFwdMsg++;
	}

	/* response to client message processing */
	// Final result print out to the screen.
	public void respondClientMsgProcess(Node node, ResClientMessage msg) {
		numAllReqFinished++;
		Library.numOperaFinished++;
		if (numAllReqFinished < numOps) {
			doRequest(node, keyLength, parameters.pid, Library.recvOverhead);
		} else {
			clientThroughput = (double) numOps
					/ (double) (CommonState.getTime() + Library.recvOverhead);
		}
		if (Library.numOperaFinished == Library.numAllOpera) {
			System.out.println("The simulation time is:"
					+ (CommonState.getTime() + Library.recvOverhead));
			
			double throughput = (double) (Library.numAllOpera)
					/ (double) (CommonState.getTime() + Library.recvOverhead)
					* 1E6;
			System.out.println("The throughput is:"	+ throughput + " ops/s");
			double latency = (throughput/numServer)/1E3;
			System.out.println("Average latency is: "+ latency + " ms");
		}
	}

	/* handle the replication message */
	public void replicaMsgProcess(Node node, ReplicaMessage msg) {
		ResReplicaMessage resRepMsg = null;
		mapData.put(msg.key, msg.value);
		resRepMsg = new ResReplicaMessage(msg.messageId, true);
		updateMaxFwdTime(Library.recvOverhead);
		fwdToProc();
		updateMaxTime(Library.procTime);
		procToFwd();
		updateMaxFwdTime(Library.sendOverhead);
		EDSimulator.add(waitTimeCal(maxFwdTime), resRepMsg, msg.sender,
				parameters.pid);
		numFwdMsg++;
	}

	/* handle the replication response message */
	public void resReplicaMsgProcess(Node sender, ResReplicaMessage msg) {
		updateMaxFwdTime(Library.recvOverhead);
		ReplicaInfo repInfo = mapReplica.get(msg.messageId);
		repInfo.numReplicaRecv++;
		if (repInfo.numReplicaRecv != numReplica) {
			doReplica(sender, repInfo.msg, repInfo.numReplicaRecv,
					msg.messageId, 0);
		} else {
			ResClientMessage resMsg = new ResClientMessage(repInfo.msg.taskId,
					1, repInfo.msg.key, repInfo.msg.value, true, true, 0);
			mapData.put(repInfo.msg.key, repInfo.msg.value);
			updateMaxFwdTime(Library.sendOverhead);
			if (repInfo.msg.sender.getIndex() != sender.getIndex()) {
				EDSimulator.add(waitTimeCal(maxFwdTime), resMsg,
						repInfo.msg.sender, parameters.pid);
			} else {
				EDSimulator.add(maxFwdTime - CommonState.getTime(), resMsg,
						repInfo.msg.sender, parameters.pid);
			}
			numFwdMsg++;
		}
	}

	public long waitTimeCal(long endTime) {
		return endTime - CommonState.getTime() + Library.commuOverhead;
	}

	public void updateMaxTime(long increment) {
		if (CommonState.getTime() > maxTime) {
			maxTime = CommonState.getTime();
		}
		maxTime += increment;
	}

	// Ke
	public void updateMaxFwdTime(long increment) {
		if (CommonState.getTime() > maxFwdTime) {
			maxFwdTime = CommonState.getTime();
		}
		maxFwdTime += increment;
	}

	// Ke
	public void procToFwd() {
		if (maxFwdTime < maxTime) {
			maxFwdTime = maxTime;
		}
	}

	// Ke
	public void fwdToProc() {
		if (maxTime < maxFwdTime) {
			maxTime = maxFwdTime;
		}
	}

	public void doGet(Node sender, int keyLength, int pid, long wait) {
		BigInteger randKey = new BigInteger(keyLength, CommonState.r);
		String key = randKey.toString();
		OperationMessage msg = new OperationMessage(0, sender, 0, key, null); 
		// get: type = 0
		long time = wait;
		int destId = hashServer(new BigInteger(key), numServer);
		if (destId != sender.getIndex()) {
			time += Library.sendOverhead + Library.commuOverhead;
		}
		createTask(sender, 0, wait);
		msg.taskId = Library.taskId;
		EDSimulator.add(time, msg, Network.get(destId), pid);
	}

	/* submit a put request */
	// Ke
	public void doPut(Node sender, int keyLength, int pid, long wait) {
		BigInteger randKey = new BigInteger(keyLength, CommonState.r);
		String key = randKey.toString();
		byte[] valueByte = new byte[134];
		CommonState.r.nextBytes(valueByte);
		String value = valueByte.toString();
		OperationMessage msg = new OperationMessage(0, sender, 1, key, value);
		// put: type = 1
		long time = wait;
		int destId = hashServer(new BigInteger(key), numServer);
		if (destId != sender.getIndex()) {
			time += Library.sendOverhead + Library.commuOverhead;
		}
		createTask(sender, 1, wait);
		msg.taskId = Library.taskId;
		EDSimulator.add(time, msg, Network.get(destId), pid);
	}

	public void createTask(Node sender, int type, long wait) {
		Library.taskId++;
	}

	/* submit a request */
	// Ke
	public void doRequest(Node sender, int keyLength, int pid, long wait) {
		double ran = CommonState.r.nextDouble();
		if (ran >= 0.5) {
			doGet(sender, keyLength, pid, wait);
		} else {
			doPut(sender, keyLength, pid, wait);
		}
	}

	/* update the times of task */
	// not used??
	public void updateTask(TaskDetail td) {
		td.taskQueuedTime = CommonState.getTime();
		td.taskEndTime = maxTime;
	}

}