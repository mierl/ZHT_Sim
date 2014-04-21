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
	public Parameters par;
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
	public HashMap<String, String> hmData;

	/* Hash Map for replication */
	public HashMap<Long, ReplicaInfo> hmReplica;

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
		this.par = new Parameters();
		this.par.tid = Configuration.getPid(prefix + "." + PARA_TRANSPORT);
		this.numServer = Configuration.getInt(prefix + "." + PARA_NUMSERVER);
		this.numOpera = Configuration.getInt(prefix + "." + PARA_NUMOPERATION);
		this.idLength = Configuration.getInt(prefix + "." + PARA_IDLENGTH);
		this.numReplica = Configuration.getInt(prefix + "." + PARA_NUMREPLICA);
	}

	public Object clone() {
		PeerProtocol pp = new PeerProtocol(this.prefix);
		return pp;
	}

	// System defined, Must implement this function, no change to function name
	// and parameters. Logic body.
	public void processEvent(Node node, int pid, Object event) {

		Library.numAllMessage++;// no much use

		if (event.getClass() == OperationMessage.class) {
			OperationMessage operationMsg = (OperationMessage) event;
			operationMsgProcess(operationMsg, node);

		} else if (event.getClass() == ResClientMessage.class) {
			ResClientMessage responClientMsg = (ResClientMessage) event;
			respondClientMsgProcess(node, responClientMsg);

		} else if (event.getClass() == ReplicaMessage.class) {
			ReplicaMessage replicaMsg = (ReplicaMessage) event;
			replicaMsgProcess(node, replicaMsg);

		} else if (event.getClass() == ResReplicaMessage.class) {
			ResReplicaMessage respondReplicaMsg = (ResReplicaMessage) event;
			resReplicaMsgProcess(node, respondReplicaMsg);
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
	public void doReplica(Node sender, OperationMessage om, int i, long messageId,
			int time) {
		String key = null, value = null;
		key = om.key;
		value = om.value;
		ReplicaMessage rm = new ReplicaMessage(sender, messageId, key, value);
		int replicaId = (sender.getIndex() + i + 1) % numServer;
		EDSimulator.add(waitTimeCal(maxFwdTime), rm, Network.get(replicaId),
				par.pid);
		numFwdMsg++;
	}

	// Ke
	public void operationMsgProcess(OperationMessage opMsg, Node node) {
		numReqRecv++;
		updateMaxFwdTime(Library.recvOverhead);
		if (numReplica == 0 || opMsg.type == 0) {
			ResClientMessage rcm = (ResClientMessage) actOperaMsgProcess(opMsg,
					node);
			procToFwd();
			updateMaxFwdTime(Library.sendOverhead);
			if (node.getIndex() != opMsg.sender.getIndex()) {
				EDSimulator.add(waitTimeCal(maxFwdTime), rcm, opMsg.sender,
						par.pid);
			} else {
				EDSimulator.add(maxFwdTime - CommonState.getTime(), rcm,
						opMsg.sender, par.pid);
			}
		} else {
			actOperaMsgProcess(opMsg, node);
		}
	}

	/* Do actual operation */
	public Object actOperaMsgProcess(OperationMessage om, Node node) {
		String value = null;
		ResClientMessage rcm = null;
		if (om.type == 0) {
			value = hmData.get(om.key);
		} else if (numReplica == 0) {
			value = om.value;
			hmData.put(om.key, om.value);
		}
		fwdToProc();
		updateMaxTime(Library.procTime);
		if (numReplica > 0 && om.type != 0) {
			messCount++;
			hmReplica.put(messCount, new ReplicaInfo(om, 0));
			doReplica(node, om, 0, messCount, 0);
			return null;
		} else {
			rcm = new ResClientMessage(om.taskId, om.type, om.key, value, true,
					true, 0);
			return rcm;
		}
	}

	/* response to client message processing */
	// Final result print out to the screen.
	public void respondClientMsgProcess(Node node, ResClientMessage rcm) {
		numAllReqFinished++;
		Library.numOperaFinished++;
		if (numAllReqFinished < numOpera) {
			doRequest(node, idLength, par.pid, Library.recvOverhead);
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
		}
	}

	/* handle the replication message */
	public void replicaMsgProcess(Node node, ReplicaMessage rm) {
		ResReplicaMessage rrm = null;
		hmData.put(rm.key, rm.value);
		rrm = new ResReplicaMessage(rm.messageId, true);
		updateMaxFwdTime(Library.recvOverhead);
		fwdToProc();
		updateMaxTime(Library.procTime);
		procToFwd();
		updateMaxFwdTime(Library.sendOverhead);
		EDSimulator.add(waitTimeCal(maxFwdTime), rrm, rm.sender, par.pid);
		numFwdMsg++;
	}

	/* handle the replication response message */
	public void resReplicaMsgProcess(Node sender, ResReplicaMessage rrm) {
		updateMaxFwdTime(Library.recvOverhead);
		ReplicaInfo ri = hmReplica.get(rrm.messageId);
		ri.numReplicaRecv++;
		if (ri.numReplicaRecv != numReplica) {
			doReplica(sender, ri.om, ri.numReplicaRecv, rrm.messageId, 0);
		} else {
			ResClientMessage rcm = new ResClientMessage(ri.om.taskId, 1,
					ri.om.key, ri.om.value, true, true, 0);
			hmData.put(ri.om.key, ri.om.value);
			updateMaxFwdTime(Library.sendOverhead);
			if (ri.om.sender.getIndex() != sender.getIndex()) {
				EDSimulator.add(waitTimeCal(maxFwdTime), rcm, ri.om.sender,
						par.pid);
			} else {
				EDSimulator.add(maxFwdTime - CommonState.getTime(), rcm,
						ri.om.sender, par.pid);
			}
			numFwdMsg++;
		}
	}

	public long waitTimeCal(long endTime) {
		return endTime - CommonState.getTime() + Library.commOverhead;
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

	public void doGet(Node sender, int idLength, int pid, long wait) {
		BigInteger ranKey = new BigInteger(idLength, CommonState.r);
		String key = ranKey.toString();
		OperationMessage om = new OperationMessage(0, sender, 0, key, null);
		long time = wait;
		int destId = hashServer(new BigInteger(key), numServer);
		if (destId != sender.getIndex()) {
			time += Library.sendOverhead + Library.commOverhead;
		}
		createTask(sender, 0, wait);
		om.taskId = Library.taskId;
		EDSimulator.add(time, om, Network.get(destId), pid);
	}

	/* submit a put request */
	// Ke
	public void doPut(Node sender, int idLength, int pid, long wait) {
		BigInteger ranKey = new BigInteger(idLength, CommonState.r);
		String key = ranKey.toString();
		byte[] valueByte = new byte[134];
		CommonState.r.nextBytes(valueByte);
		String value = valueByte.toString();
		OperationMessage om = new OperationMessage(0, sender, 1, key, value);
		long time = wait;
		int destId = hashServer(new BigInteger(key), numServer);
		if (destId != sender.getIndex()) {
			time += Library.sendOverhead + Library.commOverhead;
		}
		createTask(sender, 1, wait);
		om.taskId = Library.taskId;
		EDSimulator.add(time, om, Network.get(destId), pid);
	}

	public void createTask(Node sender, int type, long wait) {
		Library.taskId++;
	}

	/* submit a request */
	// Ke
	public void doRequest(Node sender, int idLength, int pid, long wait) {
		double ran = CommonState.r.nextDouble();
		if (ran >= 0.5) {
			doGet(sender, idLength, pid, wait);
		} else {
			doPut(sender, idLength, pid, wait);
		}
	}

	/* update the times of task */
	// not used??
	public void updateTask(TaskDetail td) {
		td.taskQueuedTime = CommonState.getTime();
		td.taskEndTime = maxTime;
	}

}