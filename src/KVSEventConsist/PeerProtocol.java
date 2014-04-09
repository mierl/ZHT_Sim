/* The protocol of peer (both server and client), it implements all the 
 * behavior of the peer
 */

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;

import java.io.IOException;
import java.math.*;
import java.util.*;

public class PeerProtocol implements EDProtocol 
{
	/* static configuration parameters read from the configuration file
	 * PARA_TRANSPORT: specify the transport layer protocol
	 * PARA_NUMSERVER: specify the number of servers
	 * PARA_NUMOPERATION: specify the number of operations per client
	 * PARA_IDLENGTH: specify the length of key space
	 * PARA_GATHERSIZE: for ctree, specify the threshold for gathering requests
	 * PARA_NUMREPLICA: specify the number of replicas
	 */
	private static final String PARA_TRANSPORT = "transport";
	private static final String PARA_NUMSERVER = "numServer";
	private static final String PARA_NUMOPERATION = "numOperation";
	private static final String PARA_IDLENGTH = "idLength";
	private static final String PARA_NUMREPLICA = "numReplica";
	private static final String PARA_MAXNUMTRY = "maxNumTry";
	private static final String PARA_NUMREADREQUIRED = "numReadRequired";
	private static final String PARA_NUMWRITEREQUIRED = "numWriteRequired";
	 
	/* values of parameters read from the configuration file, 
	 * corresponding to configuration parameters
	 * par: includes transport layer protocol id, and peer protocol id
	 * numServer: number of servers
	 * numOpera: number of operations per server
	 * idLength: the length of key space, from 0 to 2^idLength - 1
	 * gatherSize: the threshold for gathering requests for ctree
	 * numReplica: the number of replicas
	 */
	public Parameters par;
	public int numServer;
	public int numOpera;
	public int idLength;
	public int numReplica;
	public int maxNumTry;
	
	/* attributes of a peer 
	 * prefix: the prefix of configuration file
	 * id: the global id of a peer
	 * servId: for a client, which server it connects to
	 * maxTime: for a server, the occurrence time of the last operation. 
	 * 				  for the purpose of serialization of all the request 
	 */
	public String prefix;
	public BigInteger id;
	public int servId;
	public long maxTime;
	public long maxFwdTime;
	
	
	/* for dfc churn 
	 *	memList: the membership list
	 *	first: whether it is a broadcast initializer or receiver  
	 */
	public HashMap<Integer, Boolean> memList;  
	public boolean first, up, ready; 

	/*	for dchord 
	 *	predecessor: the predecessor of a server
	 *	fingerTable: the finger table in which all the nodes the server could talk directly  
	 *	succListSize: the size of successor list
	 *	succList: the successors 
	 *	messCount: for distinguishing the messages in message routing
	 */
	public Node predecessor;
	public NodeExtend[] fingerTable;	
	public int succListSize; 
	public Node[] succList;		
	public long messCount;
	
	/* Hash Map for storing the routing path */
	public HashMap<String, Integer> hmUpStreams; 
	/* Hash Map for re-trying of client */
	public HashMap<Long, Integer> hmTry;
	public HashMap<Integer, HashMap<String, Versioned>> versionedData;
	public HashMap<QuorumKey, QuorumStore> quorumStore;
	
	/* ids of the replicas*/
	public int[] replicaIds;
	
	/* Counters for statistics */
	public long numLocalReq;
	public long numLocalReqFinished;
	public long numLocalReqFailed;
	
	public long numFwdMsg;
	public long numFwdMsgFailed;
	
	public long numFwdReq;
	public long numFwdReqFailed;
	
	public long numRecFromOtherMsg;
	public long numRecFromOtherMsgFailed;
	
	public long numRecReqFromOther;
	public long numRecReqFromOtherFailed;
	
	public long numOtherReqFinished;
	public long numOtherReqFailed;
	public long numAllReqFinished;
	public long numAllReqFailed;
	public long numTaskSubmitted;
	
	public double throughput;
	public double clientThroughput;
	
	public int numReadRequired;
	public int numWriteRequired;
	
	/* initialization
	 * read the parameters from the configuration file
	 */
	public PeerProtocol(String prefix)
	{
		this.prefix = prefix;
		this.par = new Parameters();
		this.par.tid = Configuration.getPid(prefix + "." + PARA_TRANSPORT);
		this.numServer =  Configuration.getInt(prefix + "." + PARA_NUMSERVER);
		this.numOpera =  Configuration.getInt(prefix + "." + PARA_NUMOPERATION);
		this.idLength =  Configuration.getInt(prefix + "." + PARA_IDLENGTH);
		this.numReplica = Configuration.getInt(prefix + "." + PARA_NUMREPLICA);
		this.maxNumTry = Configuration.getInt(prefix + "." + PARA_MAXNUMTRY);
		this.numReadRequired = Configuration.getInt(prefix + "." + PARA_NUMREADREQUIRED);
		this.numWriteRequired = Configuration.getInt(prefix + "." + PARA_NUMWRITEREQUIRED);
	}
	
	public void processEvent(Node node, int pid, Object event)
	{
		Library.processEvent(node, pid, event);
	}
	
	public Object clone()
	{
		PeerProtocol pp = new PeerProtocol(this.prefix);
		return pp;
	}
	
	public boolean isUp()
	{
		return up;
	}
	
	public void setDown()
	{
		up = false;
	}
	
	public void setUp()
	{
		this.up = true;
	}
	
	/* create the task description upon submitting for the purpose of logging */
	public void createTask(Node sender, int type, long wait)
	{
		Library.taskId++;
		String operaType = "put";
		if (type == 0)
		{
			operaType = "get";
		}
		TaskDetail td = new TaskDetail(Library.taskId, sender.getIndex(), "yes", 
				operaType, CommonState.getTime() + wait + Library.clientSendOverhead, 0, 0, 0, 0);
		Library.taskHM.put(Library.taskId, td);
	}
	
	/* submit a request */
	public void doRequest(Node sender, Node dist, int idLength, int pid, long wait)
	{
		numTaskSubmitted++;
		int ClientIndex = sender.getIndex() - Library.offset;
		int taskIndex = (int)(numLocalReqFinished + numLocalReqFailed);
		TaskDescription td = Library.td[ClientIndex][taskIndex];
		OperaMessage om = new OperaMessage(new QuorumKey(0,1), sender, td.type, 
				td.key, td.value, dist.getIndex(), false);
		long time = wait + Library.clientSendOverhead + Library.commOverhead;
		createTask(sender, td.type, wait);
		om.qk.taskId = Library.taskId;
		EDSimulator.add(time, om, dist, pid);
	}
	
	/* update max processing time */ 
	public void updateMaxTime(long increment)
	{
		if (CommonState.getTime() > maxTime)
		{
			maxTime = CommonState.getTime();
		}
		maxTime += increment;
	}
	
	/* update max forwarding time */
	public void updateMaxFwdTime(long increment)
	{
		if (CommonState.getTime() > maxFwdTime)
		{
			maxFwdTime = CommonState.getTime();
		}
		maxFwdTime += increment;
	}
	
	/* turn from processing queue to forwarding queue */
	public void procToFwd()
	{
		if (maxFwdTime < maxTime)
		{
			maxFwdTime = maxTime;
		}
	}
	
	/* turn from forwarding queue to processing queue */
	public void fwdToProc()
	{
		if (maxTime < maxFwdTime)
		{
			maxTime = maxFwdTime;
		}
	}
	
	/* calculate how much time to wait for a message */
	public long waitTimeCal(long endTime)
	{
		return endTime - CommonState.getTime() + Library.commOverhead;
	}
	
	/* update the times of task */
	public void updateTask(TaskDetail td)
	{
		td.taskQueuedTime = CommonState.getTime();
		td.taskEndTime = maxTime;
	}
	
	/* hash to the correct server */
	public int hashServer(BigInteger key, int numServer)
	{
		return Integer.parseInt(key.mod(
				BigInteger.valueOf((long)numServer)).toString());
	}
	
	/* Judge whether a number belongs to a range */
	public boolean rangeJudge(BigInteger id,  BigInteger left, 
			BigInteger right, boolean include)
	{
		boolean flag = false;
		if (right.compareTo(left) < 0)
		{
			if (left.compareTo(id) < 0 || right.compareTo(id) > 0)
			{
				flag = true;
			}
		}
		else
		{
			if (left.compareTo(id) < 0 && right.compareTo(id) > 0)
			{
				flag = true;
			}
		}
		if (!flag && include)
		{
			if (id.compareTo(right) == 0)
			{
				flag = true;
			}
		}
		return flag;
	}
	
	/* find the closest next node */
	public Node closetPreNode(Node cur, BigInteger idKey)
	{
		for (int i = idLength - 1;i >= 0;i--)
		{
			Node node = fingerTable[i].node;
			PeerProtocol pp = (PeerProtocol)node.getProtocol(par.pid);
			if (rangeJudge(pp.id, id, idKey, false))
			{
				if (fingerTable[i].alive)
				{
					return node;
				}
				else
				{
					return null;
				}
			}
		}
		return cur;
	}
	
	public GenericObj handleObjType(Object actEvent)
	{
		QuorumKey qk = null;
		int operaType = 0;
		String key = null, value = null;
		if (actEvent.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)actEvent;
			qk = om.qk;
			operaType = om.type;
			key = om.key;
			value = om.value;
		}
		else if (actEvent.getClass() == DFCForwardMessage.class)
		{
			DFCForwardMessage fm = (DFCForwardMessage)actEvent;
			qk = fm.qk;
			operaType = fm.type;
			key = fm.key;
			value = fm.value;
		}
		else if (actEvent.getClass() == DChordOperaMessage.class)
		{
			DChordOperaMessage com = (DChordOperaMessage)actEvent;
			qk = com.qk;
			operaType = com.type;
			key = com.key;
			value = com.value;
		}
		return new GenericObj(qk, operaType, key, value);
	}
	
	public void putOperaProcessLocal(Versioned versioned, Node node, GenericObj go)
	{
		if (versioned == null)
		{
			ArrayList<ClockEntry> vc = new ArrayList<ClockEntry>();
			vc.add(new ClockEntry(node.getIndex(), 1));
			versioned = new Versioned(go.value, new VectorClock(vc, CommonState.getTime()));
			versionedData.get(node.getIndex()).put(go.key, versioned);
		}
		else
		{
			versioned.value = go.value;
			versioned.vc.incrementVersion(node.getIndex(), CommonState.getTime());
		}
	}
	
	public int[] prefList(String key)
	{
		int[] preferList = new int[numReplica + 1];
		int originId = 0, i = 0, preId = 0;
		if (Library.type == 10)
		{
			originId = hashServer(new BigInteger(key), numServer);
		}
		else
		{
			BigInteger range = Library.upperBound.divide(
					new BigInteger(Integer.toString(numServer)));
			originId = (new BigInteger(key).divide(range).intValue() + 1) % numServer;
		}
		preId = (originId - 1 + numServer) % numServer;
		while (i <= numReplica)
		{
			if (i == 0)
			{
				preferList[i] = getNextReplica(preId);
			}
			else
			{
				preferList[i] = getNextReplica(preferList[i - 1]);
			}
			i++;
		}
		return preferList;
	}
	
	public void actOperaMsgProcessEvenConsit(Object actEvent, Node node)
	{
		GenericObj go = handleObjType(actEvent);
		Versioned versioned = versionedData.get(node.getIndex()).get(go.key);
		// Put operation
		if (go.operaType == 1)
		{
			putOperaProcessLocal(versioned, node, go);
		}
		ArrayList<QuorumResMessage> al = new ArrayList<QuorumResMessage>();
		al.add(new QuorumResMessage(go.qk, node.getIndex(), versioned, true));
		quorumStore.put(go.qk, new QuorumStore(actEvent, 1, 1, al));
		fwdToProc();
		updateMaxTime(Library.procTime);
		procToFwd();
		long startTime = CommonState.getTime();
		long endTime = maxFwdTime;
		QuorumMessage qm = new QuorumMessage(go.qk, node.getIndex(), 
									go.key, versioned, go.operaType);
		int[] preferList = prefList(go.key);
		int j = 0;
		for (int i = 0;i <= numReplica;i++)
		{
			if (preferList[i] != node.getIndex())
			{
				endTime += (Library.sendOverhead * (j + 1));
				if (updateGlobalQueue(node, startTime, endTime, qm, Network.get(preferList[i])))
				{
					numFwdMsg++;
				}
				j++;
			}
		}
		updateMaxFwdTime(Library.sendOverhead * j);
	}
	
	public QuorumResMessage actQuoMsgProcess(Node node, QuorumMessage qm)
	{
		int originId = hashServer(new BigInteger(qm.key), numServer);
		HashMap<String, Versioned> hm = versionedData.get(originId);
		QuorumResMessage qrm = null;
		if (hm == null)
		{
			hm = new HashMap<String, Versioned>();
			hm.put(qm.key, qm.versioned);
			versionedData.put(originId, hm);
			qrm = new QuorumResMessage(qm.qk, node.getIndex(), qm.versioned, true);
			return qrm;
		}
		else
		{
			Versioned versioned = hm.get(qm.key);
			if (versioned == null)
			{
				hm.put(qm.key, qm.versioned);
				qrm = new QuorumResMessage(qm.qk, node.getIndex(), qm.versioned, true);
				return qrm;
			}
			else
			{
				if (qm.versioned == null)
				{
					qrm = new QuorumResMessage(qm.qk, node.getIndex(), versioned, false);
					return qrm;
				}
				Occurred occurred = versioned.vc.compare(qm.versioned.vc);
				if (occurred == Occurred.BEFORE)
				{
					hm.put(qm.key, qm.versioned);
					qrm = new QuorumResMessage(qm.qk, node.getIndex(), qm.versioned, true);
					return qrm;
				}
				else
				{
					qrm = new QuorumResMessage(qm.qk, node.getIndex(), versioned, false);
					return qrm;
				}
			}
		}
	}
	
	/* Quorum message */
	public void quoMsgProcess(Node node, QuorumMessage qm)
	{
		QuorumResMessage qrm = null;
		if (!isUp())
		{
			numRecFromOtherMsgFailed++;
			qrm = new QuorumResMessage(qm.qk, node.getIndex(), null, false);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, qrm, 
							Network.get(qm.senderId), par.pid);
		}
		else
		{
			numRecFromOtherMsg++;
			updateMaxFwdTime(Library.recvOverhead);
			fwdToProc();
			qrm = actQuoMsgProcess(node, qm);
			updateMaxTime(Library.procTime);
			procToFwd();
			updateMaxFwdTime(Library.sendOverhead);
			long startTime = CommonState.getTime();
			long endTime = maxFwdTime;
			if (updateGlobalQueue(node, startTime, endTime, qrm, Network.get(qm.senderId)))
			{
				numFwdMsg++;
			}
		}
	}
	
	public CommonResponse resQuoResponse(Node node, Object obj, boolean suc, String value)
	{
		Object objBack = null;
		int target = 0, firstReplicaId = 0;
		if (Library.type == 10)
		{
			firstReplicaId = getNextReplica(node.getIndex());
		}
		else if (Library.type == 11)
		{
			firstReplicaId = replicaIds[0];
		}
		if (obj.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)obj;
			objBack = new ResClientMessageChurn(om.qk.taskId, om.type, om.key, value, 
										suc, true, 1, node.getIndex(), firstReplicaId);
			target = om.sender.getIndex();
		}
		else if (obj.getClass() == DFCForwardMessage.class)
		{
			DFCForwardMessage dfm = (DFCForwardMessage)obj;
			objBack = new DFCResForwardMessage(dfm.qk.taskId, dfm.source, dfm.type, 
										dfm.key, value, suc, 2, node.getIndex());
			target = dfm.intermid;
		}
		else if (obj.getClass() == DChordOperaMessage.class)
		{
			DChordOperaMessage dom = (DChordOperaMessage)obj;
			objBack = new DChordResMessage(dom.qk.taskId, dom.type, dom.sender, dom.key, 
					value, suc, false, dom.numHops, node.getIndex(), firstReplicaId);
			target = dom.servId;
		}
		return new CommonResponse(objBack, target);
	}
	
	public void quoResMsgProcess(Node node, QuorumResMessage qrm)
	{
		QuorumStore qs = quorumStore.get(qrm.qk);
		if (qs == null)
		{
			return;
		}
		if (!isUp())
		{
			CommonResponse cr = resQuoResponse(node, qs.obj, false, null);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
							cr.obj, Network.get(cr.target), par.pid);
			quorumStore.remove(qrm.qk);
		}
		else
		{
			updateMaxFwdTime(Library.recvOverhead);
			qs.al.add(qrm);
			qs.allTime++;
			if (qrm.suc)
			{
				qs.sucTime++;
			}
			GenericObj go = handleObjType(qs.obj);
			CommonResponse cr = null;
			Versioned versioned = qs.al.get(0).versioned;
			String value = null;
			if (versioned != null)
			{
				value = versioned.value;
			}
			int numSuccOpera = (go.operaType == 0 ? numReadRequired : numWriteRequired);
			if (qs.sucTime == numSuccOpera)
			{
				cr = resQuoResponse(node, qs.obj, true, value);
				quorumStore.remove(qrm.qk);
			}
			else if (qs.allTime == numReplica + 1)
			{
				boolean tmp = false;
				if (go.operaType == 0)
				{
					tmp = true;
				}
				cr = resQuoResponse(node, qs.obj, tmp, value);
				quorumStore.remove(qrm.qk);
			}
			else
			{
				int j = 0;
				for (int i = 0;i < numServer;i++)
				{
					PeerProtocol pp = (PeerProtocol)Network.get(i).getProtocol(par.pid);
					if (pp.isUp())
					{
						j++;
					}
				}
				if (j < numReplica + 1)
				{
					cr = resQuoResponse(node, qs.obj, true, value);
					quorumStore.remove(qrm.qk);
				}
			}
			if (cr != null)
			{
				updateMaxFwdTime(Library.sendOverhead);
				long startTime = CommonState.getTime();
				long endTime = maxFwdTime;
				if (updateGlobalQueue(node, startTime, endTime, cr.obj, Network.get(cr.target)))
				{
					numFwdMsg++;
				}
			}
		}
	}
	
	/* the summary log message processing */
	public void logEventProc()
	{
		Library.log(CommonState.getTime(), numServer);
		if (Library.numOperaFinished + Library.numOperaFailed < Library.numAllOpera)
		{
			EDSimulator.add(Library.logInterval, new LogMessage(0), Network.get(0), par.pid);
		}
		else
		{
			try
			{
				Library.bwSummary.flush();
				Library.bwSummary.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public void resClientMsgProcessRepChurn(Node node, String path, ResClientMessageChurn rcmc)
	{
		TaskDetail td = Library.taskHM.get(rcmc.taskId);
		if (!rcmc.success)
		{
			OperaMessage om = new OperaMessage(new QuorumKey(rcmc.taskId, 1), node, rcmc.type, 
												rcmc.key, rcmc.value, servId, false);
			int target = rcmc.curId, time = 0;
			if (hmTry.containsKey(td.taskId))
			{
				Integer numTry = hmTry.get(td.taskId);
				numTry++;
				hmTry.put(td.taskId, numTry);
				if (numTry == maxNumTry)
				{
					target = rcmc.firstReplicaId;
					hmTry.remove(td.taskId);
					time = 1;
				}
				else
				{
					time = numTry + 1;
				}
			}
			else
			{
				hmTry.put(td.taskId, 1);
				time = 2;
			}
			om.qk.time = time;
			EDSimulator.add(Library.clientRecvOverhead + Library.clientSendOverhead + 
					Library.commOverhead, om, Network.get(target), par.pid);
		}
		else
		{
			td.taskBackClientTime = CommonState.getTime();
			td.numHops = rcmc.numHops;
			numLocalReqFinished++;
			Library.numOperaFinished++;
			td.taskSuccess = "yes";
			Library.logTask(td);
			Library.taskHM.remove(rcmc.taskId);
			int nodeId = node.getIndex();
			if (nodeId == Library.offset)
			{
				Library.logTask(nodeId, false, par.pid, 0);
			}
			if (numLocalReqFinished  < numOpera)
			{
				doRequest(node, Network.get(servId), idLength, 
								par.pid, Library.clientRecvOverhead);
				if (nodeId == Library.offset)
				{
					Library.logTask(nodeId, true, par.pid, Library.clientRecvOverhead + 
										Library.clientSendOverhead);
				}
			}
			else 
			{
				if (nodeId == Library.offset)
				{
					try
					{
						Library.bwZeroTask.flush();
						Library.bwZeroTask.close();
						TurnaroundTime tt = Library.turnaroundTime(path, numOpera);
						Library.printTAT(tt);
					}
					catch (IOException e)
					{
						e.printStackTrace();
					}
				}
				clientThroughput = (double)numOpera * 1E6 / (double)
					(CommonState.getTime() + Library.clientRecvOverhead);
			}
			if (Library.numOperaFinished == Library.numAllOpera)
			{
				try 
				{
					Library.bwSumStat.write(Long.toString(CommonState.getTime() + 
											Library.clientRecvOverhead) + "\t");
					Library.printClientThr(CommonState.getTime() + Library.clientRecvOverhead);
					Library.calServThroughput(par.pid);
					Library.logServStatistics(par.pid);
					Library.printServStatistics(par.pid);
					Library.printLoad(par.pid);
					Library.logClientStatistics(Network.size() - Library.offset, par.pid);
					Library.printFailRate(numServer, par.pid, numOpera);
					Library.printChurn();
					Library.bwSumStat.write(Library.numAllMessage + "\t" + Library.numOperaMsg + "\t" 
							+ Library.numChurnMsg + "\t" + Library.numStrongConsistMsg 
							+ "\t" + Library.numEvenConsistMsg + "\r\n");
					Library.bwTask.flush();
					Library.bwTask.close();
					Library.bwSumStat.flush();
					Library.bwSumStat.close();
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	/* When churn happens, update its own waiting queue */
	public void updateSelfWaitQueue(Node node)
	{
		LinkedList<WaitingEvent> ll = Library.waitQueue.get(node.getIndex());
		while (ll.size() > 0)
		{
			WaitingEvent we = ll.removeFirst();
			Object ev = we.event;
			if (ev.getClass() == ResClientMessageChurn.class)
			{
				ResClientMessageChurn rcmc = (ResClientMessageChurn)ev;
				rcmc.success = false;
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
							rcmc, Network.get(we.toNode), par.pid);
				numFwdMsgFailed++;
				if (rcmc.cur)
				{
					numLocalReqFailed++;
				}
			}
			else if (ev.getClass() == DFCForwardMessage.class)
			{
				DFCForwardMessage fm = (DFCForwardMessage)ev;
				int firstReplicaId = getNextReplica(node.getIndex());
				ResClientMessageChurn rcmc = new ResClientMessageChurn(fm.qk.taskId, fm.type,
				fm.key, fm.value, false, true, fm.numHops, node.getIndex(), firstReplicaId);
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
								rcmc, fm.source, par.pid);
				numFwdMsgFailed++;
				numFwdReqFailed++;
			}
			else if (ev.getClass() == DFCResForwardMessage.class)
			{
				DFCResForwardMessage rfm = (DFCResForwardMessage)ev;
				rfm.success = false;
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, rfm,
										Network.get(we.toNode), par.pid);
				numFwdMsgFailed++;
				numOtherReqFailed++;
			}
			else if (ev.getClass() == NetChurnMessage.class)
			{
				numFwdMsgFailed++;
			}
			else if (ev.getClass() == MemMessage.class)
			{
				MemMessage mm = (MemMessage)ev;
				mm.success = false;
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, mm,
										Network.get(we.toNode), par.pid);
				numFwdMsgFailed++;
			}
			if (ev.getClass() == DChordResForwardMessage.class)
			{
				DChordResForwardMessage rfm = (DChordResForwardMessage)ev;
				rfm.success = false;
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, rfm, 
										Network.get(we.toNode), par.pid);
				numFwdMsgFailed++;
			}
			else if (ev.getClass() == ResLookUpMessage.class)
			{
				ResLookUpMessage rlm = (ResLookUpMessage)ev;
				rlm.success = false;
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, rlm, 
										Network.get(we.toNode), par.pid);
				numFwdMsgFailed++;
			}
			else if (ev.getClass() == DChordResMessage.class)
			{
				DChordResMessage rm = (DChordResMessage)ev;
				rm.success = false;
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, rm, 
										Network.get(we.toNode), par.pid);
				numFwdMsgFailed++;
				numOtherReqFailed++;
			}
			else if (ev.getClass() == DChordForwardMessage.class)
			{
				DChordForwardMessage ofm = (DChordForwardMessage)ev;
				DChordResForwardMessage rfm = new DChordResForwardMessage(ofm.qk.taskId, 
						ofm.messageId, ofm.type, ofm.key, ofm.value, ofm.numHops, null, false);
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, rfm, 
										ofm.sender, par.pid);
				numFwdMsgFailed++;
			}
			else if (ev.getClass() == LookUpSuccMessage.class)
			{
				LookUpSuccMessage lsm = (LookUpSuccMessage)ev;
				ResLookUpMessage rlm = new ResLookUpMessage(lsm.messageId, lsm.type, lsm.i, 
															lsm.key, lsm.numHops, null, false);
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
									rlm, lsm.sender, par.pid);
				numFwdMsgFailed++;
			}
			else if (ev.getClass() == DChordOperaMessage.class)
			{
				DChordOperaMessage om = (DChordOperaMessage)ev;
				int firstReplicaId = (Library.type == 10 ? getNextReplica(
						node.getIndex()) : replicaIds[0]);
				ResClientMessageChurn rcmc = new ResClientMessageChurn(om.qk.taskId, om.type, 
				om.key, om.value, false, true, om.numHops, node.getIndex(), firstReplicaId);
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
							rcmc, om.sender, par.pid);
				numFwdMsgFailed++;
				numFwdReqFailed++;
			}
			else if (ev.getClass() == DataResForwardMessage.class)
			{
				DataResForwardMessage drfm = (DataResForwardMessage)ev;
				drfm.success = false;
				EDSimulator.add(Library.sendOverhead + 
						Library.commOverhead, drfm, Network.get(we.toNode), par.pid);
			}
			else if (ev.getClass() == QuorumMessage.class)
			{
				QuorumMessage qm = (QuorumMessage)ev;
				QuorumStore qs = quorumStore.get(qm.qk);
				if (qs == null)
				{
					continue;
				}
				CommonResponse cr = resQuoResponse(node, qs.obj, false, null);
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
								cr.obj, Network.get(cr.target), par.pid);
				quorumStore.remove(qm.qk);
			}
			else if (ev.getClass() == QuorumResMessage.class)
			{
				QuorumResMessage qrm = (QuorumResMessage)ev;
				qrm.suc = false;
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
								qrm, Network.get(we.toNode), par.pid);
			}
		}
		maxTime = CommonState.getTime();
		maxFwdTime = CommonState.getTime();
	}
	
	/* When churn happens, update other servers' waiting queues */
	public void updateOthersWaitQueue(int nodeId)
	{
		for (int i = 0;i < numServer;i++)
		{
			if (i != nodeId)
			{
				updateServerWaitQueue(i);
			}
		}
	}
	
	public void updateServerWaitQueue(int i)
	{
		PeerProtocol pp = (PeerProtocol)(Network.get(i).getProtocol(par.pid));
		LinkedList<WaitingEvent> ll = Library.waitQueue.get(i);
		int size = ll.size();
		int j = 0;
		int k = 0;
		while (j < size)
		{
			WaitingEvent we = ll.get(j - k);
			Object ev = we.event;
			if (we.endTime <= Library.currChurnTime || 
					(we.startTime >= Library.currChurnTime && 
						we.endTime <= Library.nextChurnTime))
			{
				ll.remove(j - k);
				EDSimulator.add(we.endTime + we.extra - CommonState.getTime(), 
										ev, Network.get(we.toNode), par.pid);
				k++;
				pp.numFwdMsg++;
				if (ev.getClass() == ResClientMessage.class)
				{
					ResClientMessage rcm = (ResClientMessage)ev;
					if (rcm.cur)
					{
						pp.numLocalReqFinished++;
					}
				}
				else if (ev.getClass() == DFCResForwardMessage.class)
				{
					pp.numOtherReqFinished++;
				}
				else if (ev.getClass() == DChordOperaMessage.class)
				{
					pp.numFwdReq++;
				}
				else if (ev.getClass() == DChordResMessage.class)
				{
					DChordResMessage rm = (DChordResMessage)ev;
					if (rm.success && rm.first)
					{
						pp.numOtherReqFinished++;
					}
				}
			}
			j++;
		}
	}
	
	/* update the global waiting queue */
	public boolean updateGlobalQueue(Node node, long startTime, 
			long endTime, Object event, Node receiver)
	{
		if (endTime <= Library.currChurnTime || startTime >= 
			Library.currChurnTime && endTime <= Library.nextChurnTime)
		{
			EDSimulator.add(waitTimeCal(endTime), event, receiver, par.pid);
			return true;
		}
		else
		{
			Library.waitQueue.get(node.getIndex()).add(new WaitingEvent(receiver.getIndex(), 
					startTime, endTime, Library.commOverhead, event));
			return false;
		}
	}
	
	/* Process the churn event */
	public void churnNotMsgProcess(Node node)
	{
		if (Library.numOperaFinished + Library.numOperaFailed == Library.numAllOpera)
		{
			return;
		}
		maxTime = CommonState.getTime();
		maxFwdTime = CommonState.getTime();
		boolean type = false;
		if (!isUp())
		{
			type = true;
		}
		if (Library.type == 10)
		{
			Library.dfcChurn(type, node, par.pid);
		}
		else if (Library.type == 11)
		{
			Library.dchordChurn(type, node, idLength, par.pid);
		}
		if (type)
		{
			Library.numJoins++;
			Library.numActiveServer++;
		}
		else
		{
			Library.numLeaves++;
			Library.numActiveServer--;
			ready = false;
		}
		updateSelfWaitQueue(node);
		Library.currChurnTime = Library.nextChurnTime;
		Library.nextChurnTime += Library.churnInterval;
		updateOthersWaitQueue(node.getIndex());
		if (Library.numOperaFinished + Library.numOperaFailed < Library.numAllOpera)
		{
			Node nextChurnNode = Library.selChurnNode(numServer);
			EDSimulator.add(Library.churnInterval, 
					new ChurnNoticeMessage(), nextChurnNode, par.pid);
		}
	}
	
	public int getReplicaId(Node node, int i)
	{
		int replicaId = 0;
		int index = node.getIndex();
		if (i == 0)
		{
			replicaId = (index + i + 1) % numServer;
		}
		else
		{
			replicaId = (replicaIds[i - 1] + 1) % numServer;
		}
		while (!memList.containsKey(replicaId))
		{
			replicaId = (replicaId + 1) % numServer;
		}
		return replicaId;
	}
	
	public int getNextReplica(int originId)
	{
		int replicaId = (originId + 1) % numServer;
		if (Library.type == 10)
		{
			while (!memList.containsKey(replicaId))
			{
				replicaId = (replicaId + 1) % numServer;
			}
		}
		else
		{
			PeerProtocol pp = (PeerProtocol)(Network.get(replicaId).getProtocol(par.pid));
			while (!pp.isUp())
			{
				replicaId = (replicaId + 1) % numServer;
				pp = (PeerProtocol)(Network.get(replicaId).getProtocol(par.pid));
			}
		}
		return replicaId;
	}
	
	public int ithReplicaForth(int curNodeIdx, int churnNodeIdx)
	{
		int diff = (curNodeIdx - churnNodeIdx + numServer) % numServer;
		int ithReplica = 0;
		for (int i = 1;i < diff;i++)
		{
			if (memList.containsKey((churnNodeIdx + i) % numServer))
			{
				ithReplica++;
			}
		}
		ithReplica++;
		if (ithReplica > numReplica)
		{
			return -1;
		}
		else
		{
			return ithReplica;
		}
	}
	
	public int ithReplicaBack(int curNodeIdx, int churnNodeIdx)
	{
		int diff = (churnNodeIdx - curNodeIdx + numServer) % numServer;
		int ithReplica = 0;
		for (int i = 1;i < diff;i++)
		{
			if (memList.containsKey((curNodeIdx + i) % numServer))
			{
				ithReplica++;
			}
		}
		ithReplica++;
		if (ithReplica > numReplica)
		{
			return -1;
		}
		else
		{
			return ithReplica;
		}
	}
	
	public void sendData(int senderId, int dataResideId, int receiveId, boolean all)
	{
		long startTime = CommonState.getTime();
		updateMaxFwdTime(Library.sendOverhead);
		long endTime = maxFwdTime;
		HashMap<Integer, HashMap<String, Versioned>> hmAllDataToSend = new 
							HashMap<Integer, HashMap<String, Versioned>>();
		HashMap<String, Versioned> hmDataToSend = versionedData.get(dataResideId);
		if (!all)
		{
			hmAllDataToSend.put(dataResideId, hmDataToSend);
		}
		else
		{
			Iterator<Integer> it = versionedData.keySet().iterator();
			while (it.hasNext())
			{
				int id = it.next();
				if (id != dataResideId)
				{
					hmAllDataToSend.put(id, versionedData.get(id));
				}
			}
		}
		DataForwardMessage dfm = new DataForwardMessage(
				senderId, hmAllDataToSend, all, replicaIds);
		if (updateGlobalQueue(Network.get(senderId), 
				startTime, endTime, dfm, Network.get(receiveId)))
		{
			numFwdMsg++;
		}
	}
	
	public void fixReplicaLeave(int curNodeIdx, int churnNodeIdx, int ithReplicaF, int ithReplicaB)
	{
		int receiveId = 0;
		if (ithReplicaF == 1)
		{
			receiveId = replicaIds[numReplica - 1];
			sendData(curNodeIdx, churnNodeIdx, receiveId, false);
		}
		if (ithReplicaB != -1)
		{
			receiveId = (churnNodeIdx + numReplica - ithReplicaB + 1) % numServer;
			while (!memList.containsKey(receiveId))
			{
				receiveId = (receiveId + 1) % numServer;
			}
			int[] temp = new int[numReplica];
			for (int i = 0;i < numReplica;i++)
			{
				temp[i] = replicaIds[i];
			}
			if (receiveId != curNodeIdx)
			{
				sendData(curNodeIdx, curNodeIdx, receiveId, false);
				replicaIds[numReplica - 1] = receiveId;
			}
			for (int i = ithReplicaB - 1;i < numReplica - 1;i++)
			{
				replicaIds[i] = temp[i + 1];
			}
		}
	}
	
	public void fixReplicaJoin(int curNodeIdx, int churnNodeIdx, int ithReplicaF, int ithReplicaB)
	{
		if (ithReplicaF == 1)
		{
			sendData(curNodeIdx, curNodeIdx, churnNodeIdx, true);
		}
		else if (ithReplicaF == -1 && versionedData.containsKey(churnNodeIdx))
		{
			versionedData.remove(churnNodeIdx);
		}
		if (ithReplicaB != -1)
		{
			int[] temp = new int[numReplica];
			for (int i = 0;i < numReplica;i++)
			{
				temp[i] = replicaIds[i];
			}
			replicaIds[ithReplicaB - 1] = churnNodeIdx;
			for (int i = ithReplicaB;i < numReplica;i++)
			{
				replicaIds[i] = temp[i - 1];
			}
			long startTime = CommonState.getTime();
			updateMaxFwdTime(Library.sendOverhead);
			long endTime = maxFwdTime;
			if (updateGlobalQueue(Network.get(curNodeIdx), 
					startTime, endTime, new DataDeleteMessage(curNodeIdx), 
					Network.get(temp[numReplica - 1])))
			{
				numFwdMsg++;
			}
		}
	}
	
	public void fixReplica(int curNodeIdx, int churnNodeIdx, boolean type)
	{
		int ithReplicaF = ithReplicaForth(curNodeIdx, churnNodeIdx);
		int ithReplicaB = ithReplicaBack(curNodeIdx, churnNodeIdx);
		if (!type)
		{
			fixReplicaLeave(curNodeIdx, churnNodeIdx, ithReplicaF, ithReplicaB);
		}
		else
		{
			fixReplicaJoin(curNodeIdx, churnNodeIdx, ithReplicaF, ithReplicaB);
		}
	}
	
	public void updateFinTable()
	{
		BigInteger base = BigInteger.ZERO;
		ArrayList<Integer> al = new ArrayList<Integer>();
		for (int i = 0;i < numServer;i++)
		{
			PeerProtocol pp = (PeerProtocol)Network.get(i).getProtocol(par.pid);
			if (pp.isUp())
			{
				al.add(i);
			}
		}
		for (int i = 1;i <= idLength;i++)
		{
			if (i == 1)
			{
				base = BigInteger.ONE;
			}
			else
			{
				base = base.multiply(new BigInteger("2"));
			}
			BigInteger bi = id.add(base).mod(Library.upperBound);
			for (int j = 0;j < al.size();j++)
			{
				BigInteger left = ((PeerProtocol)Network.get(al.get(j)).getProtocol(par.pid)).id;
				BigInteger right = ((PeerProtocol)Network.get
						(al.get((j + 1) % al.size())).getProtocol(par.pid)).id;
				if (rangeJudge(bi, left, right, true))
				{
					fingerTable[i - 1].node = Network.get(al.get((j + 1) % al.size()));
					fingerTable[i - 1].alive = true;
					if (i - 1 < succListSize)
					{
						succList[i - 1] = Network.get(al.get((j + 1) % al.size()));
					}
					break;
				}
			}
		}
	}
}