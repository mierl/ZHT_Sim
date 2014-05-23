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
	private static final String PARA_proxyRate = "proxyRate";
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
	public int numServerPerGroup; // num of servers in a group, connect to one
									// proxy
	public int numOps;
	public int keyLength; // it was named idLength
	public int numReplica;
	private int proxyRate;
	public Roles roles;
	/*
	 * attributes of a peer: prefix: the prefix of configuration file. id: the
	 * global id of a peer servId: for a client, which server it connects to
	 * maxTime: for a server, the occurrence time of the last operation. for the
	 * purpose of serialization of all the request
	 */

	public String prefix; // the prefix of configuration file
	public BigInteger id;// not used???
	// the global id of a peer servId: for a client, which server it connects
	// to.

	public long maxProcessQTime;// for a server, the time of last operation. for
								// the
	// purpose of serialization of all the request
	public long maxCommuQTime;// ???

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

	// Hash Map for storing the (key, value) data
	public HashMap<String, String> mapData;

	// Hash Map for checking replicas
	public HashMap<Long, ReplicaInfo> mapReplica;

	/* Counters for statistics */
	public long numReqRecv;

	public long numFwdMsg;

	public long numAllReqFinished;

	public double throughput;
	public double clientThroughput;

	public ArrayList<Node> childrenNodes;

	/*
	 * initialization read the parameters from the configuration file
	 */

	// constructor of above: read file and assignment/init
	// Only the first obj is create by using this, all others are by clone().
	// 每个PP instance都和一个Node一一对应,但pid是和protocol类型关联, 目前只有一个protocol.
	public PeerProtocol(String prefix) {
		this.prefix = prefix;
		this.parameters = new Parameters();
		this.parameters.tid = Configuration.getPid(prefix + "."
				+ PARA_TRANSPORT);
		this.numServer = Configuration.getInt(prefix + "." + PARA_NUMSERVER);
		this.numOps = Configuration.getInt(prefix + "." + PARA_NUMOPERATION);
		this.keyLength = Configuration.getInt(prefix + "." + PARA_IDLENGTH);
		this.numReplica = Configuration.getInt(prefix + "." + PARA_NUMREPLICA);
		this.proxyRate = Configuration.getInt(prefix + "." + PARA_proxyRate);
		this.numServerPerGroup = proxyRate;
		this.roles = new Roles();
	}

	public Object clone() {
		PeerProtocol pp = new PeerProtocol(this.prefix);
		return pp;
	}

	// System defined, Must implement this function, no change to function name
	// and parameters. Logic body.
	// node:

	/**
	 * This method is invoked by the scheduler to deliver events to the
	 * protocol. Apart from the event object, information about the node and the
	 * protocol identifier are also provided. Additional information can be
	 * accessed through the {@link CommonState} class.
	 * 
	 * @param localNode
	 *            the local node
	 * @param pid
	 *            the identifier of this protocol
	 * @param event
	 *            the delivered event
	 */
	// node: event的接收者, pid是此protocol的class id, event

	public int findProxy(String strKey, int numProxy) {
		BigInteger key = new BigInteger(strKey);
		return Integer.parseInt(key.mod(BigInteger.valueOf((long) numProxy))
				.toString());

		// int key = Integer.parseInt(strKey);
		// return key % numProxy;

	}

	public int findServer(String strKey) {
		BigInteger key = new BigInteger(strKey);
		// int key = Integer.parseInt(strKey);
		return Integer.parseInt((key.mod(BigInteger.valueOf((long) numServer))
				.mod(BigInteger.valueOf((long) this.numServerPerGroup)))
				.toString());
		// return (key % numServer) % numServerInGroup;
	}

	public int sendAck(Node sender, Node dest, int pid, int state) {
		AckMsg msg = new AckMsg(0, null, sender, state);// logic wrong ...
		long delay = 0;// ?? not sure
		int destId = dest.getIndex();
		/*
		 * if (destId != sender.getIndex()) {// if this is a remote request
		 * delay += Library.sendOverhead + Library.commuOverhead; }
		 */
		// createTask(sender, 1, delay);
		msg.taskId = Library.taskId;
		// EDSimulator.add(long delay, Object event, Node receiver, int pid)

		if (destId != sender.getIndex()) {// if this is a remote request
			// time += Library.sendOverhead + Library.commuOverhead; works, old
			delay = waitTimeCal(maxCommuQTime);
		} else {// local request
			delay = maxCommuQTime - CommonState.getTime();
		}
		EDSimulator.add(delay, msg, Network.get(destId), pid);
		// addEventToSchedulerHierarchy(sender, msg, pid, 0, 4);
		return 0;
	}

	public void localProcess() {
		commuQToProcQ();
		updateProcessQMaxTime(Library.procTime);
	}

	public void processEvent(Node localNode, int pid, Object event) {
		// System.out.println("Node.getID= " + node.getID());
		// Library.numAllMessage++;// no much use

		updateMaxCommuQTime(Library.recvOverhead);
		// all msg receiving have this cost
		int ackState = 0; // 0 for success
		if (event.getClass() == ClientReq.class) {
			// System.out.println("----------------ClientReq");
			debugPrintTime();
			// proxy get a request from a client
			// ClientReq msg = (ClientReq) event;
			localProcess();

			ClientReq oriMsg = (ClientReq) event;

			ProxyForwardMsg forwardMsg = new ProxyForwardMsg(0,
					oriMsg.initialSender, localNode, ackState, oriMsg.key,
					oriMsg.value);
			// int destServerID = findServer(forwardMsg.key, numServerInGroup);
			addEventToSchedulerHierarchy(localNode, forwardMsg, pid,
					Library.recvOverhead, 2);
			// 直接转发请求

			sendAck(localNode, oriMsg.initialSender, pid, ackState);
			debugPrintTime();

		} else if (event.getClass() == PackMsg.class) {
			// System.out.println("----------------PackMsg");

			// proxy/server get a batch request, both should work the same way:
			// TODO: tell role, proxy or server
			if (this.roles.isProxy) {
				// TODO: if proxy: regroup ...
				// TODO: regroup(), return a list of new packs
				// TODO: counting # of msg for each node in childrenNodes
				// TODO: counting time for each node in childrenNodes
				// TODO: Judge when sending new pack to children nodes: strategy

			} else if (this.roles.isServer) {
				// TODO: if server: answer request

			}

			// TODO: send packs

		} else if (event.getClass() == ProxyForwardMsg.class) {
			// System.out.println("----------------ProxyForwardMsg");

			// server get a msg from proxy, send msg to client,
			ProxyForwardMsg msg = (ProxyForwardMsg) event;
			ServerAnswerMsg answer;
			// put/get
			if (0 == msg.type) {// get
				String value = mapData.get(msg.key);
				answer = new ServerAnswerMsg(0, msg.initialSender, localNode,
						localNode.getIndex(), msg.key, value, 0);
			} else if (1 == msg.type) {// put
				mapData.put(msg.key, msg.value);
				answer = new ServerAnswerMsg(0, msg.initialSender, localNode,
						localNode.getIndex(), null, null, 0);
			} else {// should never run into this: system design error.
				answer = new ServerAnswerMsg(0, msg.initialSender, localNode,
						localNode.getIndex(), null, null, -99);
				System.out
						.println("\n\n\n ------------------ PeerProtocol.java: processEvent(): Design error!! --------------\n\n\n");

			}

			localProcess();
			addEventToSchedulerHierarchy(localNode, answer, pid,
					Library.recvOverhead, 3);
			// sendAck()??

			// no need to sendAck() here.

		} else if (event.getClass() == ServerAnswerMsg.class) {
			// client get answer from server
			// System.out.println("----------------ServerAnswerMsg");
			ServerAnswerMsg msg = (ServerAnswerMsg) event;
			this.numAllReqFinished++;
			Library.numOperaFinished++;
			// System.out.println("initRequest: Library.numOperaFinished = " +
			// Library.numOperaFinished);
			// System.out.println("continueRequests... Peer ID = "+
			// this.id+", local: numAllReqFinished: "+ numAllReqFinished +
			// ", numOps: " + numOps);

			continueRequests(localNode);
			// after get result, continue send request.

			// Network.get(msg.senderServerID)
			sendAck(localNode, msg.currentSender, pid, ackState);
			// xxxx - wrong. 原来的发送端信息丢失. 应发回给server, 此处是发给client自己

		} else if (event.getClass() == AckMsg.class) {
			// System.out.println("----------------AckMsg");

			// upon receiving ack msg, do nothing. 暂不考虑出错重发

		}

		else if (event.getClass() == OperationMsg.class) {
			// System.out.println("=====================OperationMsg====================");
			OperationMsg msg = (OperationMsg) event;
			operationMsgProcess(msg, localNode);
		}

		else if (event.getClass() == ResClientMsg.class) {
			// System.out.println("=====================ResClientMsg====================");

			ResClientMsg msg = (ResClientMsg) event;
			respondClientMsgProcess(localNode, msg);
			// executed by client,node: client which recv ack

		} else if (event.getClass() == ReplicaMsg.class) {
			ReplicaMsg msg = (ReplicaMsg) event;
			replicaMsgProcess(localNode, msg);

		} else if (event.getClass() == ResReplicaMsg.class) {
			ResReplicaMsg msg = (ResReplicaMsg) event;
			resReplicaMsgProcess(localNode, msg);
		}
	}

	public void addEventToSchedulerHierarchy(Node sender, OperationMsg msg,
			int pid, long delay, int mode) {
		long time = delay;
		int destId = 0;
		// updateMaxCommuQTime(Library.recvOverhead);
		procQToCommuQ();
		if (1 == mode) { // mode 1: client发出给proxy
			destId = findProxy(msg.key, numServer / proxyRate);
		} else if (2 == mode) { // mode 2: proxy转发给server
			// updateMaxCommuQTime(Library.sendOverhead);
			destId = findServer(msg.key);
		} else if (3 == mode || 4 == mode) {
			// updateMaxCommuQTime(Library.sendOverhead);
			// mode 3: server发出给client 结果; mode 4: all ack
			destId = msg.initialSender.getIndex();
		} else {// should never run into this.
			// System.out
			// .println("\n\n\n ------------------ PeerProtocol.java: addEventToSchedulerHierarchy(): Design error!! --------------\n\n\n");
			destId = 0;
		}
		updateMaxCommuQTime(Library.sendOverhead);

		/*
		 * if (destId != sender.getIndex()) {// if this is a remote request time
		 * += Library.sendOverhead + Library.commuOverhead; }
		 */

		if (destId != sender.getIndex()) {// if this is a remote request
			// time += Library.sendOverhead + Library.commuOverhead; works, old
			time = waitTimeCal(maxCommuQTime);
		} else {// local request
			time = maxCommuQTime - CommonState.getTime();
		}
		// createTask(sender, 1, delay);
		msg.taskId = Library.taskId;
		// EDSimulator.add(long delay, Object event, Node receiver, int pid)
		EDSimulator.add(time, msg, Network.get(destId), pid);
	}

	/*
	 * public void simpleSendAckUpdate(Node sender, Node receiver, Object
	 * resMsg) { long latency = 0; updateMaxFwdTime(Library.sendOverhead); if
	 * (receiver.getIndex() != sender.getIndex()) { latency =
	 * waitTimeCal(maxFwdTime); } else { latency = maxFwdTime -
	 * CommonState.getTime(); }
	 * 
	 * EDSimulator.add(latency, resMsg, receiver, parameters.pid); numFwdMsg++;
	 * }
	 */

	// create the task description upon submitting for the purpose of logging
	// hash to the correct server
	public int hashServer(BigInteger key, int numServer) {
		return Integer.parseInt(key.mod(BigInteger.valueOf((long) numServer))
				.toString());
	}

	// Send messages to other replicas for updating
	public void operationMsgProcess(OperationMsg opMsg, Node sender) {
		numReqRecv++;

		// updateMaxCommuQTime(Library.recvOverhead);

		if (numReplica == 0 || opMsg.type == 0) { // type 0: get, 1: put
			ResClientMsg responseMsg = (ResClientMsg) actOpMsgProcess(opMsg,
					sender);
			procQToCommuQ();// another time update
			updateMaxCommuQTime(Library.sendOverhead);

			// get之后要返回给client, client即是收到的msg的initialSender
			if (sender.getIndex() != opMsg.initialSender.getIndex()) {// ???
				// 1st para: 当前 到 事件实际完成的时刻 的时间差
				EDSimulator.add(waitTimeCal(maxCommuQTime), responseMsg,
						opMsg.initialSender, parameters.pid);
			} else {// 因为本地, 没有commuOverhead 故maxCommuQTime -
					// CommonState.getTime()
				EDSimulator.add(maxCommuQTime - CommonState.getTime(),
						responseMsg, opMsg.initialSender, parameters.pid);
			}
		} else {// replica > 0, or put
			actOpMsgProcess(opMsg, sender);
		}
	}

	// Do actual operation
	public Object actOpMsgProcess(OperationMsg opMsg, Node sender) {
		String value = null;
		ResClientMsg resMsg = null;
		if (opMsg.type == 0) { // type=0, get
			value = mapData.get(opMsg.key);
		} else if (numReplica == 0) {
			value = opMsg.value;
			mapData.put(opMsg.key, opMsg.value); // type!=0, put
		}
		commuQToProcQ();// ?? no need here?
		updateProcessQMaxTime(Library.procTime);

		if (numReplica > 0 && opMsg.type != 0) {// put
			messCount++;
			mapReplica.put(messCount, new ReplicaInfo(opMsg, 0));
			doReplica(sender, opMsg, 0, messCount, 0);
			return null;

		} else {// get, or no replica
			resMsg = new ResClientMsg(opMsg.taskId, opMsg.type, opMsg.key,
					value, true, true, 0);
			return resMsg;
		}
	}

	public void doReplica(Node sender, OperationMsg msg, int i, long messageId,
			int time) {
		String key = null, value = null;
		key = msg.key;
		value = msg.value;
		ReplicaMsg repMsg = new ReplicaMsg(sender, messageId, key, value);
		int replicaId = (sender.getIndex() + i + 1) % numServer;
		EDSimulator.add(waitTimeCal(maxCommuQTime), repMsg,
				Network.get(replicaId), parameters.pid);
		numFwdMsg++;
	}

	// response to client message processing
	// Final result print out to the screen.
	public void respondClientMsgProcess(Node node, ResClientMsg msg) {
		numAllReqFinished++;
		Library.numOperaFinished++;
		if (numAllReqFinished < numOps) {
			doRequest(node, keyLength, parameters.pid, Library.recvOverhead);
		} else {
			clientThroughput = (double) numOps
					/ (double) (CommonState.getTime() + Library.recvOverhead);
		}

		// Finished, summary of data
		if (Library.numOperaFinished == Library.numAllOpera) {
			System.out.println("The simulation time is:"
					+ (CommonState.getTime() + Library.recvOverhead));

			double throughput = (double) (Library.numAllOpera)
					/ (double) (CommonState.getTime() + Library.recvOverhead)
					* 1E6;
			System.out.println("Library.commuOverhead = "
					+ Library.commuOverhead);
			System.out.println("The throughput is:" + throughput + " ops/s on "
					+ numServer + " nodes.");
			double latency = 1000 * 1 / (throughput / numServer);
			// double latency = (double)(1/clientThroughput)/1000;
			System.out.println("Average latency is: " + latency + " ms");
		}
	}

	// handle the replication message 处理replica请求,并回应replica操作请求以ack
	public void replicaMsgProcess(Node node, ReplicaMsg msg) {
		ResReplicaMsg resRepMsg = null;
		mapData.put(msg.key, msg.value);
		resRepMsg = new ResReplicaMsg(msg.messageId, true);
		updateMaxCommuQTime(Library.recvOverhead);
		commuQToProcQ();
		updateProcessQMaxTime(Library.procTime);
		procQToCommuQ();
		updateMaxCommuQTime(Library.sendOverhead);
		EDSimulator.add(waitTimeCal(maxCommuQTime), resRepMsg, msg.sender,
				parameters.pid);
		// res: 回应请求, replica处理请求就是put然后发个回应, 这个回应相当于ack, 应该发给原来消息的发起人
		numFwdMsg++;
	}

	// handle the replication response message
	// replica请求发出者收到replica发回的ack
	public void resReplicaMsgProcess(Node sender, ResReplicaMsg msg) {
		updateMaxCommuQTime(Library.recvOverhead);
		ReplicaInfo repInfo = mapReplica.get(msg.messageId);
		repInfo.numReplicaRecv++;
		if (repInfo.numReplicaRecv != numReplica) {
			// 若没有更新完全部replica, 再做一次, 直到全部更新
			doReplica(sender, repInfo.msg, repInfo.numReplicaRecv,
					msg.messageId, 0);
		} else {// 已更新完.
			ResClientMsg resMsg = new ResClientMsg(repInfo.msg.taskId, 1,
					repInfo.msg.key, repInfo.msg.value, true, true, 0);// put消息
			mapData.put(repInfo.msg.key, repInfo.msg.value);// 先更新replica,再更新自己?
			updateMaxCommuQTime(Library.sendOverhead);

			// 更新完所有replica后, 回应client以ack
			if (repInfo.msg.initialSender.getIndex() != sender.getIndex()) {
				EDSimulator.add(waitTimeCal(maxCommuQTime), resMsg,
						repInfo.msg.initialSender, parameters.pid);
			} else {
				EDSimulator.add(maxCommuQTime - CommonState.getTime(), resMsg,
						repInfo.msg.initialSender, parameters.pid);
			}
			numFwdMsg++;
		}
	}

	public long waitTimeCal(long scheduledTime) {
		return scheduledTime - CommonState.getTime() + Library.commuOverhead;
	}

	public void updateProcessQMaxTime(long increment) {
		if (CommonState.getTime() > maxProcessQTime) {
			maxProcessQTime = CommonState.getTime();
		}
		maxProcessQTime += increment;
	}

	public void updateMaxCommuQTime(long increment) {
		if (CommonState.getTime() > maxCommuQTime) {
			maxCommuQTime = CommonState.getTime();
		}
		maxCommuQTime += increment;
	}

	public void procQToCommuQ() {// max
		if (maxCommuQTime < maxProcessQTime) {
			maxCommuQTime = maxProcessQTime;
		}
	}

	public void commuQToProcQ() {// move request from commu to proc
		// Q_process Q_commu

		if (maxProcessQTime < maxCommuQTime) {
			maxProcessQTime = maxCommuQTime;
		}
	}

	public void debugPrintTime() {
		// System.out.println("============DEBUG_INFO: maxCommuQTime = " +
		// maxCommuQTime + ", maxProcessQTime = " + maxProcessQTime);
	}

	public void doGet(Node sender, int keyLength, int pid, long delay) {
		String key = randKey(keyLength);
		OperationMsg msg = new OperationMsg(0, sender, 0, key, null);
		addInitClientReqToScheduler(sender, msg, pid, delay);
		// get: type = 0
	}

	// submit a put request
	// delay: the time delay before the event is scheduled.
	// pid: the id of protocol to which the event will be sent
	public void doPut(Node sender, int keyLength, int pid, long delay) {
		int valueLength = 134;
		String key = randKey(keyLength);
		String value = randValue(valueLength);
		// make event: msg; put: type = 1, what about 1st parameter taskID??
		OperationMsg msg = new OperationMsg(0, sender, 1, key, value);
		addInitClientReqToScheduler(sender, msg, pid, delay);
	}

	// update time, find target server, and add event to scheduler
	public void addInitClientReqToScheduler(Node sender, OperationMsg msg,
			int pid, long delay) {
		long time = delay;
		int destId = hashServer(new BigInteger(msg.key), numServer);
		if (destId != sender.getIndex()) {// if this is a remote request
			// time += Library.sendOverhead + Library.commuOverhead; works, old
			time = waitTimeCal(maxCommuQTime);
		} else {// local request
			time = maxCommuQTime - CommonState.getTime();
		}
		createTask(sender, 1, delay);
		msg.taskId = Library.taskId;
		// EDSimulator.add(long delay, Object event, Node receiver, int pid)
		EDSimulator.add(time, msg, Network.get(destId), pid);
	}

	public void initRequest(Node sender, int keyLength, int pid, long delay) {
		// similar with doRequest()
		int valueLength = 134;
		String key = randKey(keyLength);
		String value;

		double rand = CommonState.r.nextDouble();
		int opType = -1;
		if (rand >= 0.5) {
			opType = 0;
			value = null;
		} else {
			opType = 1;
			value = randValue(valueLength);
		}

		ClientReq reqMsg = new ClientReq(0, sender, sender, opType, key, value);
		int mode = 1; // mode 1: client submit request
		addEventToSchedulerHierarchy(sender, reqMsg, pid, delay, mode);
	}

	// similcar with respondClientMsgProcess()
	public void continueRequests(Node sender) {
		// ugly design.
		// System.out.println("continueRequests...");

		// System.out.println("continueRequests... Peer ID = "+
		// this.id+", local: numAllReqFinished: "+ numAllReqFinished +
		// ", numOps: " + numOps);
		// System.out.println("initRequest: Library.numOperaFinished = " +
		// Library.numOperaFinished);

		if (this.numAllReqFinished < this.numOps) {// count for this node
			// System.out.println("this.numAllReqFinished: " +
			// this.numAllReqFinished +", this.numOps:"+ this.numOps);
			double rand = CommonState.r.nextDouble();

			initRequest(sender, keyLength, parameters.pid, Library.recvOverhead);
		} else {
			// System.out.println("continueRequests done, summrize.......................................");
			clientThroughput = (double) numOps
					/ (double) (CommonState.getTime() + Library.recvOverhead);
			// Finished, summary of data
			// System.out.println("\n\n\n continueRequests... Library.numOperaFinished: "+
			// Library.numOperaFinished + ", Library.numAllOpera: " +
			// Library.numAllOpera);
			if (Library.numOperaFinished == Library.numAllOpera) {
				System.out.println("ZHT-H Simulation: network.size = "
						+ Network.size() + ", proxyRate = " + proxyRate);
				System.out.println("Library.commuOverhead = "
						+ Library.commuOverhead);

				System.out.println("Proxy number = " + numServer / proxyRate
						+ ", numServerInGroup = " + numServerPerGroup);
				System.out.println("The simulation time is:"
						+ (CommonState.getTime() + Library.recvOverhead));

				double throughput = (double) (Library.numAllOpera)
						/ (double) (CommonState.getTime() + Library.recvOverhead)
						* 1E6;
				System.out.println("The throughput is:" + throughput
						+ " ops/s on " + numServer + " nodes.");
				// double latency = 1000 * 1 / (throughput / numServer);
				double latency = (double) (1 / clientThroughput) / 1000;

				System.out.println("Average latency is: " + latency + " ms");
			}
		}
	}

	public String randKey(int keyLength) {
		BigInteger randKey = new BigInteger(keyLength, CommonState.r);
		return randKey.toString();
	}

	public String randValue(int valueLength) {
		byte[] valueByte = new byte[134];
		CommonState.r.nextBytes(valueByte);
		return valueByte.toString();
	}

	public void createTask(Node sender, int type, long wait) {
		Library.taskId++;
	}

	// submit a request, also called from TrafficGene
	// 随机发出put/get, 产生request, 而不是处理request
	public void doRequest(Node sender, int keyLength, int pid, long wait) {
		double rand = CommonState.r.nextDouble();
		if (rand >= 0.5) {
			doGet(sender, keyLength, pid, wait);
		} else {
			doPut(sender, keyLength, pid, wait);
		}
	}

	// update the times of task
	// not used??
	public void updateTask(TaskDetail td) {
		td.taskQueuedTime = CommonState.getTime();
		td.taskEndTime = maxProcessQTime;
	}

}