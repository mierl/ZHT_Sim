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
	private static final String PARA_GATHERSIZE = "gatherSize";
	private static final String PARA_NUMREPLICA = "numReplica";
	private static final String PARA_MAXNUMTRY = "maxNumTry";
	 
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
	public int gatherSize;
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
	
	public ArrayList<OperaMessage> al;	  // for ctree, storing the gathered requests
	
	// for ctree, storing the backup requests in case the server is dead
	public ArrayList<OperaMessage> alBackUp;  
	public long count;
	
	/* for dfc churn 
	 *	memList: the membership list
	 *	first: whether it is a broadcast initializer or receiver  
	 */
	public HashMap<Integer, Boolean> memList;  
	public boolean first; 
	
	public boolean up;  	// for dfc and dchord churn, whether a server is up or down  
	public boolean ready;
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
	
	/* Hash Map for storing the (key, value) data */
	public HashMap<String, String> hmData;	
	
	/* Hash Map for storing the routing path */
	public HashMap<String, Integer> hmUpStreams; 
	
	/* Hash Map for replication */
	public HashMap<Long, ReplicaInfo> hmReplica;
	
	/* Hash Map for re-trying of client */
	public HashMap<Long, Integer> hmTry;
	public HashMap<Long, Integer> hmRepTry;
	
	/* Hash Map for all the replicated data */
	public HashMap<Integer, HashMap<String, String>> hmAllData;
	
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
	
	public double throughput;
	public double clientThroughput;
	
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
		this.gatherSize =  Configuration.getInt(prefix + "." + PARA_GATHERSIZE);
		this.numReplica = Configuration.getInt(prefix + "." + PARA_NUMREPLICA);
		this.maxNumTry = Configuration.getInt(prefix + "." + PARA_MAXNUMTRY);
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
	
	/* submit a get request */
	public void doGet(Node sender, int distId, int idLength, int pid, long wait)
	{
		BigInteger ranKey = new BigInteger(idLength, CommonState.r);
		String key = ranKey.toString();
		OperaMessage om = new OperaMessage(0, sender, 0, key, null, distId, false);
		long time = wait + Library.clientSendOverhead + Library.commOverhead;
		createTask(sender, 0, wait);
		om.taskId = Library.taskId;
		EDSimulator.add(time, om, Network.get(distId), pid);
	}
	
	/* submit a put request */
	public void doPut(Node sender, int distId, int idLength, int pid, long wait)
	{
		BigInteger ranKey = new BigInteger(idLength, CommonState.r);
		String key = ranKey.toString();
		byte[] valueByte = new byte[100];
		CommonState.r.nextBytes(valueByte);
		String value = valueByte.toString();
		OperaMessage om = new OperaMessage(0, sender, 1, key, value, distId, false);
		long time = wait + Library.clientSendOverhead + Library.commOverhead;
		createTask(sender, 1, wait);
		om.taskId = Library.taskId;
		EDSimulator.add(time, om, Network.get(distId), pid);
	}
	
	/* submit a request */
	public void doRequest(Node sender, Node dist, int idLength, int pid, long wait)
	{
		int ClientIndex = sender.getIndex() - Library.offset;
		int taskIndex = (int)(numLocalReqFinished + numLocalReqFailed);
		TaskDescription td = Library.td[ClientIndex][taskIndex];
		OperaMessage om = new OperaMessage(0, sender, td.type, 
				td.key, td.value, dist.getIndex(), false);
		long time = wait + Library.clientSendOverhead + Library.commOverhead;
		createTask(sender, td.type, wait);
		om.taskId = Library.taskId;
		EDSimulator.add(time, om, dist, pid);
		/*double ran = CommonState.r.nextDouble();
		if (ran >= 0.5)
		{
			doGet(sender, dist.getIndex(), idLength, pid, wait);
		}
		else
		{
			doPut(sender, dist.getIndex(), idLength, pid, wait);
		}*/
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
	
	public int getReplicaId(Node sender, int i, int time, int recvId)
	{
		int replicaId = 0;
		if (Library.type == 0 || Library.type == 1 || Library.type == 6 || Library.type == 7)
		{
			replicaId = sender.getIndex() + i + 1;
		}
		else if (Library.type == 2)
		{
			replicaId = (sender.getIndex() + i + 1) % numServer;
		}
		else if (Library.type == 3)
		{
			replicaId = succList[i].getIndex();
		}
		else if (Library.type == 8 || Library.type == 9)
		{
			if (time < maxNumTry)
			{
				replicaId = recvId;
			}
			else if (Library.type == 8)
			{
				replicaId = getNextReplica(recvId);
			}
			else
			{
				PeerProtocol ppTemp = (PeerProtocol)Network.get(recvId).getProtocol(par.pid);
				replicaId =ppTemp.succList[0].getIndex();
				PeerProtocol pp = (PeerProtocol)ppTemp.succList[0].getProtocol(par.pid);
				while (!pp.isUp())
				{
					replicaId =pp.succList[0].getIndex();
					pp = (PeerProtocol)Network.get(replicaId).getProtocol(par.pid);
				}
			}
		}
		return replicaId;
	}
	
	/* Send messages to other replicas for updating */
	public void doReplica(Node sender, Object obj, int i, long messageId, int time, int recvId)
	{
		GenericObj go = handleObjType(obj);
		ReplicaMessage rm = new ReplicaMessage(sender, messageId, go.key, go.value);
		int replicaId = getReplicaId(sender, i, time, recvId);
		if (i == 0)
		{
			procToFwd();
		}
		updateMaxFwdTime(Library.sendOverhead);
		if (Library.type > 5)
		{
			long startTime = CommonState.getTime();
			long endTime = maxFwdTime;
			if (updateGlobalQueue(sender, startTime, endTime, rm, Network.get(replicaId)))
			{
				numFwdMsg++;
			}
		}
		else
		{
			EDSimulator.add(waitTimeCal(maxFwdTime), rm, Network.get(replicaId), par.pid);
			numFwdMsg++;
		}
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
		long taskId = 0;
		int operaType = 0;
		String key = null, value = null;
		if (actEvent.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)actEvent;
			taskId = om.taskId;
			operaType = om.type;
			key = om.key;
			value = om.value;
		}
		else if (actEvent.getClass() == DFCForwardMessage.class)
		{
			DFCForwardMessage fm = (DFCForwardMessage)actEvent;
			taskId = fm.taskId;
			operaType = fm.type;
			key = fm.key;
			value = fm.value;
		}
		else if (actEvent.getClass() == DChordOperaMessage.class)
		{
			DChordOperaMessage com = (DChordOperaMessage)actEvent;
			taskId = com.taskId;
			operaType = com.type;
			key = com.key;
			value = com.value;
		}
		return new GenericObj(taskId, operaType, key, value);
	}
	
	public void getMsgProcess(GenericObj go)
	{
		if (Library.type < 8)
		{
			go.value = hmData.get(go.key);
		}
		else
		{
			int serverId = hashServer(new BigInteger(go.key), numServer);
			if (hmAllData.containsKey(serverId))
			{
				HashMap<String, String> hm = hmAllData.get(serverId);
				if (hm != null)
				{
					go.value = hm.get(go.key);
				}
				else
				{
					go.value = null;
				}
			}
			else
			{
				go.value = null;
			}
		}
	}
	
	/* Do actual operation */
	public Object actOperaMsgProcess(Object actEvent, Node node)
	{
		ResClientMessage rcm = null;
		DFCResForwardMessage rfm = null;
		DChordResMessage crm = null;
		GenericObj go = handleObjType(actEvent);
		if (go.operaType == 0)
		{
			getMsgProcess(go);
		}
		else if (numReplica == 0 || (Library.type == 7 && node.getIndex() == 1) 
				|| (Library.type != 7 && node.getIndex() >= numServer))
		{
			hmData.put(go.key, go.value);
		}
		fwdToProc();
		updateMaxTime(Library.procTime);
		TaskDetail td = Library.taskHM.get(go.taskId);
		updateTask(td);
		if (numReplica > 0 && go.operaType != 0 && ((Library.type == 7 && node.getIndex() == 0)
				|| (Library.type != 7 && node.getIndex() < numServer)))
		{
			messCount++;
			hmReplica.put(messCount, new ReplicaInfo(actEvent, 0));
			int recvId = 0;
			if (Library.type < 8)
			{
				recvId = -1;
			}
			else
			{
				recvId = replicaIds[0];
			}
			doReplica(node, actEvent, 0, messCount, 0, recvId);
			return null;
		}
		else
		{
			if (actEvent.getClass() == OperaMessage.class)
			{
				OperaMessage om = (OperaMessage)actEvent;
				rcm = new ResClientMessage(om.taskId, om.type, om.key, go.value, true, true, 0);
				return rcm;
			}
			else if (actEvent.getClass() == DFCForwardMessage.class)
			{
				DFCForwardMessage fm = (DFCForwardMessage)actEvent;
				rfm = new DFCResForwardMessage(fm.taskId, fm.source, fm.type, fm.key, 
										go.value, true, fm.numHops, node.getIndex());
				return rfm;
			}
			else if (actEvent.getClass() == DChordOperaMessage.class)
			{
				DChordOperaMessage com = (DChordOperaMessage)actEvent;
				int replicaId = -1;
				if (Library.type == 9)
				{
					replicaId = replicaIds[0];
				}
				crm = new DChordResMessage(com.taskId, com.type, com.sender, com.key, 
						go.value, true, true, com.numHops, node.getIndex(), replicaId);
				return crm;
			}
		}
		return null;
	}
	
	/* the summary log message processing */
	public void logEventProc() throws IOException
	{
		Library.log(CommonState.getTime(), numServer);
		if (Library.numOperaFinished + Library.numOperaFailed < Library.numAllOpera)
		{
			EDSimulator.add(Library.logInterval, new LogMessage(0), Network.get(0), par.pid);
		}
		else
		{
			Library.bwSummary.flush();
			Library.bwSummary.close();
		}
	}
	
	/* response message to client under the replication and churn property for csingle and ctree*/
	public void resClientMsgProcessRepChurn(Node node, String path, ResClientMessage rcm)
	{
		TaskDetail td = Library.taskHM.get(rcm.taskId);
		if (!rcm.success)
		{
			OperaMessage om = new OperaMessage(rcm.taskId, 
					node, rcm.type, rcm.key, rcm.value, servId, false);
			int target = servId;
			if (Library.type != 7)
			{
				if (hmTry.containsKey(td.taskId))
				{
					Integer numTry = hmTry.get(td.taskId);
					numTry++;
					hmTry.put(td.taskId, numTry);
					if (numTry == maxNumTry)
					{
						target++;
						hmTry.remove(td.taskId);
					}
				}
				else
				{
					hmTry.put(td.taskId, 1);
				}
				EDSimulator.add(Library.clientRecvOverhead + Library.clientSendOverhead + 
						Library.commOverhead, om, Network.get(target), par.pid);
			}
			else
			{
				EDSimulator.add(Library.clientRecvOverhead + Library.clientSendOverhead + 
						Library.commOverhead, om, Network.get(target), par.pid);
			}
			return;
		}
		else
		{
			td.taskBackClientTime = CommonState.getTime();
			td.numHops = rcm.numHops;
			numLocalReqFinished++;
			Library.numOperaFinished++;
			td.taskSuccess = "yes";
		}
		Library.logTask(td);
		Library.taskHM.remove(rcm.taskId);
		int nodeId = node.getIndex();
		if (nodeId == Library.offset)
		{
			Library.logTask(nodeId, false, par.pid, 0);
		}
		if (numLocalReqFinished  < numOpera)
		{
			doRequest(node, Network.get(servId), idLength, par.pid, Library.clientRecvOverhead);
			if (nodeId == Library.offset)
			{
				Library.logTask(nodeId, true, par.pid, 
						Library.clientRecvOverhead + Library.clientSendOverhead);
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
				Library.bwSumStat.write(Long.toString
						(CommonState.getTime() + Library.clientRecvOverhead) + "\t");
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
	
	/* response to client message processing */
	public void resClientMsgProcess(Node node, String path, ResClientMessage rcm)
	{
		TaskDetail td = Library.taskHM.get(rcm.taskId);
		td.taskBackClientTime = CommonState.getTime();
		td.numHops = rcm.numHops;
		if (!rcm.success)
		{
			numLocalReqFailed++;
			Library.numOperaFailed++;
			td.taskSuccess = "no";
		}
		else
		{
			numLocalReqFinished++;
			Library.numOperaFinished++;
			td.taskSuccess = "yes";
		}
		Library.logTask(td);
		Library.taskHM.remove(rcm.taskId);
		int nodeId = node.getIndex();
		if (nodeId == Library.offset)
		{
			Library.logTask(nodeId, false, par.pid, 0);
		}
		if (numLocalReqFinished + numLocalReqFailed < numOpera)
		{
			doRequest(node, Network.get(servId), idLength, par.pid, Library.clientRecvOverhead);
			if (nodeId == Library.offset)
			{
				Library.logTask(nodeId, true, par.pid, 
									Library.clientRecvOverhead + Library.clientSendOverhead);
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
			clientThroughput = (double)numLocalReqFinished / (double)
					(CommonState.getTime() + Library.clientRecvOverhead);
		}
		if (Library.numOperaFinished + Library.numOperaFailed == Library.numAllOpera)
		{
			try 
			{
				Library.bwSumStat.write(Long.toString
						(CommonState.getTime() + Library.clientRecvOverhead) + "\t");
				Library.printClientThr(CommonState.getTime() + Library.clientRecvOverhead);
				Library.calServThroughput(par.pid);
				Library.logServStatistics(par.pid);
				Library.printServStatistics(par.pid);
				Library.printLoad(par.pid);
				Library.logClientStatistics(Network.size() - Library.offset, par.pid);
				Library.printFailRate(numServer, par.pid, numOpera);
				Library.printChurn();
				Library.bwSumStat.write(Library.numAllMessage + "\r\n");
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
	
	public void resClientMsgProcessRepChurn2(Node node, ResClientMessageChurn rcmc)
	{
		TaskDetail td = Library.taskHM.get(rcmc.taskId);
		OperaMessage om = new OperaMessage(rcmc.taskId, node, 
				rcmc.type, rcmc.key, rcmc.value, servId, false);
		int target = rcmc.curId;
		if (hmTry.containsKey(td.taskId))
		{
			Integer numTry = hmTry.get(td.taskId);
			numTry++;
			hmTry.put(td.taskId, numTry);
			if (numTry == maxNumTry)
			{
				target = rcmc.firstReplicaId;
				hmTry.remove(td.taskId);
			}
		}
		else
		{
			hmTry.put(td.taskId, 1);
		}
		EDSimulator.add(Library.clientRecvOverhead + Library.clientSendOverhead + 
					Library.commOverhead, om, Network.get(target), par.pid);
		return;
	}
	
	/* When churn happens, update its own waiting queue */
	public void updateSelfWaitQueue(Node node)
	{
		LinkedList<WaitingEvent> ll = Library.waitQueue.get(node.getIndex());
		while (ll.size() > 0)
		{
			WaitingEvent we = ll.removeFirst();
			Object ev = we.event;
			if (ev.getClass() == ResClientMessage.class)
			{
				ResClientMessage rcm = (ResClientMessage)ev;
				if (Library.type > 7)
				{
					int firstReplicaId = 0;
					if (Library.type == 8)
					{
						firstReplicaId = getNextReplica(node.getIndex());
					}
					else
					{
						firstReplicaId = replicaIds[0];
					}
					ResClientMessageChurn rcmc = new ResClientMessageChurn(rcm.taskId, rcm.type, rcm.key, 
													rcm.value, false, rcm.cur, rcm.numHops, node.getIndex(), firstReplicaId);
					EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
							rcmc, Network.get(we.toNode), par.pid);
				}
				else
				{
					rcm.success = false;
					EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
													rcm, Network.get(we.toNode), par.pid);
				}
				numFwdMsgFailed++;
				if (rcm.cur)
				{
					numLocalReqFailed++;
				}
				Library.taskHM.get(rcm.taskId).taskEndTime = CommonState.getTime();
			}
			else if (ev.getClass() == DFCForwardMessage.class)
			{
				DFCForwardMessage fm = (DFCForwardMessage)ev;
				if (Library.type <= 7)
				{
					ResClientMessage rcm = new ResClientMessage(fm.taskId, fm.type, 
												fm.key, fm.value, false, true, fm.numHops);
					EDSimulator.add(Library.sendOverhead + Library.commOverhead, rcm, 
										fm.source, par.pid);
				}
				else
				{
					int firstReplicaId = 0; //
					if (Library.type == 8)
					{
						firstReplicaId = getNextReplica(node.getIndex());
					}
					else
					{
						firstReplicaId = replicaIds[0];
					}
					ResClientMessageChurn rcmc = new ResClientMessageChurn(fm.taskId, fm.type, fm.key, 
													fm.value, false, true, fm.numHops, node.getIndex(), firstReplicaId);
					EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
							rcmc, fm.source, par.pid);
				}
				Library.taskHM.get(fm.taskId).taskEndTime = CommonState.getTime();
				numFwdMsgFailed++;
				numFwdReqFailed++;
			}
			else if (ev.getClass() == DFCResForwardMessage.class)
			{
				DFCResForwardMessage rfm = (DFCResForwardMessage)ev;
				rfm.success = false;
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, rfm, 
										Network.get(we.toNode), par.pid);
				Library.taskHM.get(rfm.taskId).taskEndTime = CommonState.getTime();
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
				DChordResForwardMessage rfm = new DChordResForwardMessage(ofm.taskId, 
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
				if (Library.type <= 7)
				{
					ResClientMessage rcm = new ResClientMessage(om.taskId, 
						om.type, om.key, om.value, false, true, om.numHops);
					EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
										rcm, om.sender, par.pid);
				}
				else
				{
					int firstReplicaId = 0;
					if (Library.type == 8)
					{
						firstReplicaId = getNextReplica(node.getIndex());
					}
					else
					{
						firstReplicaId = replicaIds[0];
					}
					ResClientMessageChurn rcmc = new ResClientMessageChurn(om.taskId, om.type, om.key, 
													om.value, false, true, om.numHops, node.getIndex(), firstReplicaId);
					EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
							rcmc, om.sender, par.pid);
				}
				numFwdMsgFailed++;
				numFwdReqFailed++;
			}
			else if (ev.getClass() == ReplicaMessage.class)
			{
				ReplicaMessage rm = (ReplicaMessage)ev;
				ReplicaInfo ri = hmReplica.get(rm.messageId);
				if (ri == null)
				{
					continue;
				}
				Object obj = null;
				Node dist = null;
				if (ri.obj.getClass() == OperaMessage.class)
				{
					OperaMessage om = (OperaMessage)ri.obj;
					if (Library.type <= 7)
					{
						obj = new ResClientMessage(om.taskId, 
							om.type, om.key, om.value, false, true, 0);
					}
					else
					{
						int firstReplicaId = 0;
						if (Library.type == 8)
						{
							firstReplicaId = getNextReplica(node.getIndex());
						}
						else
						{
							firstReplicaId = replicaIds[0];
						}
						obj = new ResClientMessageChurn(om.taskId, om.type, om.key, 
														om.value, false, true, 0, node.getIndex(), firstReplicaId);
					}
					dist = om.sender;
					numLocalReqFailed++;
				}
				else if (ri.obj.getClass() == DFCForwardMessage.class)
				{
					DFCForwardMessage fm = (DFCForwardMessage)ri.obj;
					obj = new DFCResForwardMessage(fm.taskId, fm.source, 
							fm.type, fm.key, fm.value, false, fm.numHops, node.getIndex());
					dist = Network.get(fm.intermid);
					numOtherReqFailed++;
				}
				else if (ri.obj.getClass() == DChordOperaMessage.class)
				{
					DChordOperaMessage com = (DChordOperaMessage)ri.obj;
					obj = new DChordResMessage(com.taskId, com.type, com.sender, com.key,
							com.value, false, false, com.numHops, node.getIndex(), 
							replicaIds[0]);
					dist = Network.get(com.servId);
					numOtherReqFailed++;
				}
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
						obj, dist, par.pid);
			}
			else if (ev.getClass() == ReloadDataMessage.class)
			{
				numFwdMsgFailed++;
			}
			else if (ev.getClass() == CTreeResForwardMessage.class)
			{
				CTreeResForwardMessage rfm = (CTreeResForwardMessage)ev;
				numFwdMsgFailed++;
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
						rfm, Network.get(we.toNode), par.pid);
			}
			else if (ev.getClass() == DataResForwardMessage.class)
			{
				DataResForwardMessage drfm = (DataResForwardMessage)ev;
				drfm.success = false;
				EDSimulator.add(Library.sendOverhead + 
						Library.commOverhead, drfm, Network.get(we.toNode), par.pid);
			}
			else if (ev.getClass() == ResClientMessageChurn.class)
			{
				EDSimulator.add(Library.sendOverhead + 
						Library.commOverhead, ev, Network.get(we.toNode), par.pid);
			}
			else if (ev.getClass() == ResReplicaMessage.class)
			{
				ResReplicaMessage rrm = (ResReplicaMessage)ev;
				rrm.success = false;
				EDSimulator.add(Library.sendOverhead + 
						Library.commOverhead, rrm, Network.get(we.toNode), par.pid);
			}
			else if (ev.getClass() == TransReqMessage.class)
			{
				TransReqMessage trm = (TransReqMessage)ev;
				for (int i = 0;i < trm.al.size();i++)
				{
					Object obj = trm.al.get(i);
					if (obj.getClass() == OperaMessage.class)
					{
						OperaMessage om = (OperaMessage)obj;
						int firstReplicaId = getNextReplica(node.getIndex());
						ResClientMessageChurn rcmc = new ResClientMessageChurn(om.taskId, om.type, 
								om.key, om.value, false, true, 1, node.getIndex(), firstReplicaId);
						EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
								rcmc, om.sender, par.pid);
					}
					else if (obj.getClass() == DFCForwardMessage.class)
					{
						DFCForwardMessage fm = (DFCForwardMessage)obj;
						DFCResForwardMessage rfm = new DFCResForwardMessage(fm.taskId, fm.source, fm.type, 
								fm.key, fm.value, false, 2, node.getIndex());
						EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
								rfm, Network.get(fm.intermid), par.pid);
					}
					else if (obj.getClass() == DChordOperaMessage.class)
					{
						DChordOperaMessage com = (DChordOperaMessage)obj;
						DChordResMessage crm = new DChordResMessage(com.taskId, com.type, 
								com.sender, com.key, com.value,false, true, com.numHops, 
								node.getIndex(), replicaIds[0]);
						EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
								crm, Network.get(com.servId), par.pid);
					}
				}
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
		if (Library.type == 4 || Library.type == 8)
		{
			Library.dfcChurn(type, node, par.pid);
		}
		else if (Library.type == 5 || Library.type == 9)
		{
			Library.dchordChurn(type, node, idLength, par.pid);
		}
		else if (Library.type == 6 || Library.type == 7)
		{
			Library.csinglechurn(type, node, par.pid);
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
	
	/* handle the replication message */
	public void replicaMsgProcess(Node node, ReplicaMessage rm)
	{
		ResReplicaMessage rrm = null;
		if (!isUp())
		{
			rrm = new ResReplicaMessage(rm.messageId, false, node.getIndex());
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, rrm, rm.sender, par.pid);
			return;
		}
		long startTime = CommonState.getTime();
		hmData.put(rm.key, rm.value);
		rrm = new ResReplicaMessage(rm.messageId, true, node.getIndex());
		updateMaxFwdTime(Library.recvOverhead);
		fwdToProc();
		updateMaxTime(Library.procTime);
		procToFwd();
		updateMaxFwdTime(Library.sendOverhead);
		long endTime = maxFwdTime;
		if (Library.type > 7)
		{
			if (updateGlobalQueue(node, startTime, endTime, rrm, rm.sender))
			{
				numFwdMsg++;
			}
		}
		else
		{
			EDSimulator.add(waitTimeCal(maxFwdTime), rrm, rm.sender, par.pid);
			numFwdMsg++;
		}
	}
	
	public void resReplicaMsgProcessRepChurn(PeerProtocol pp, Node sender, ResReplicaMessage rrm)
	{
		if (!hmReplica.containsKey(rrm.messageId))
		{
			return;
		}
		ReplicaInfo ri = hmReplica.get(rrm.messageId);
		Object obj = ri.obj;
		Object objBack = null;
		Node dist = null;
		String key = null, value = null;
		if (obj.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)obj;
			ResClientMessage rcm = new ResClientMessage(
					om.taskId, 1, om.key, om.value, true, true, 0);
			objBack = rcm;
			dist = om.sender;
			key = om.key;
			value = om.value;
		}
		else if (obj.getClass() == DFCForwardMessage.class)
		{
			DFCForwardMessage fm = (DFCForwardMessage)obj;
			DFCResForwardMessage rfm = new DFCResForwardMessage(fm.taskId, fm.source, fm.type, 
					fm.key, fm.value, true, fm.numHops, sender.getIndex());
			objBack = rfm;
			dist = Network.get(fm.intermid);
			key = fm.key;
			value = fm.value;
		}
		else if (obj.getClass() == DChordOperaMessage.class)
		{
			DChordOperaMessage com = (DChordOperaMessage)obj;
			DChordResMessage crm = new DChordResMessage(com.taskId, com.type, com.sender, com.key, 
						com.value, true, true, com.numHops, sender.getIndex(), 
						pp.replicaIds[0]);
			objBack = crm;
			dist = Network.get(com.servId);
			key = com.key;
			value = com.value;
		}
		if (!pp.isUp())
		{
			numRecFromOtherMsgFailed++;
			if (objBack.getClass() == ResClientMessage.class)
			{
				ResClientMessage rcm = (ResClientMessage)objBack;
				if (Library.type > 7)
				{
					int firstReplicaId = 0;
					if (Library.type == 8)
					{
						firstReplicaId = pp.getNextReplica(sender.getIndex());
					}
					else
					{
						firstReplicaId = pp.replicaIds[0];
					}
					ResClientMessageChurn rcmc = new ResClientMessageChurn(rcm.taskId, rcm.type, rcm.key, 
							rcm.value, false, rcm.cur, rcm.numHops, sender.getIndex(), firstReplicaId);
					objBack = rcmc;
				}
				else
				{
					rcm.success = false;
					objBack = rcm;
				}
			}
			else if (objBack.getClass() == DFCResForwardMessage.class)
			{
				DFCResForwardMessage rfm = (DFCResForwardMessage)objBack;
				rfm.success = false;
				objBack = rfm;
			}
			else if (objBack.getClass() == DChordResMessage.class)
			{
				DChordResMessage rm = (DChordResMessage)objBack;
				rm.success = false;
				objBack = rm;
			}
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, objBack, dist, par.pid);
			return;
		}
		updateMaxFwdTime(Library.recvOverhead);
		if (rrm.success)
		{
			hmRepTry.remove(rrm.messageId);
			ri.numReplicaRecv++;
			if (ri.numReplicaRecv != numReplica)
			{
				doReplica(sender, ri.obj, ri.numReplicaRecv, rrm.messageId, 
						0, replicaIds[ri.numReplicaRecv]);
			}
			else
			{
				hmData.put(key, value);
				updateMaxFwdTime(Library.sendOverhead);
				long startTime = CommonState.getTime();
				long endTime = maxFwdTime;
				hmReplica.remove(rrm.messageId);
				if (updateGlobalQueue(sender, startTime, endTime, objBack, dist))
				{
					if (obj.getClass() == OperaMessage.class)
					{
						numLocalReqFinished++;
					}
					else
					{
						numOtherReqFinished++;
					}
					numFwdMsg++;
				}
			}
		}
		else
		{
			Integer numTry = null;
			if (hmRepTry.containsKey(rrm.messageId))
			{
				numTry = hmRepTry.get(rrm.messageId);
				numTry++;
				hmRepTry.put(rrm.messageId, numTry);
				if (numTry == maxNumTry)
				{
					hmRepTry.remove(rrm.messageId);
				}
			}
			else
			{
				hmRepTry.put(rrm.messageId, 1);
				numTry = 1;
			}
			doReplica(sender, ri.obj, ri.numReplicaRecv, rrm.messageId, numTry, rrm.curRepId);
		}
	}
	
	/* handle the replication response message */
	public void resReplicaMsgProcess(Node sender, ResReplicaMessage rrm)
	{
		updateMaxFwdTime(Library.recvOverhead);
		ReplicaInfo ri = hmReplica.get(rrm.messageId);
		ri.numReplicaRecv++;
		if (ri.numReplicaRecv != numReplica)
		{
			doReplica(sender, ri.obj, ri.numReplicaRecv, rrm.messageId, 0, -1);
		}
		else
		{
			Object obj = ri.obj;
			Object objBack = null;
			Node dist = null;
			String key = null, value = null;
			if (obj.getClass() == OperaMessage.class)
			{
				OperaMessage om = (OperaMessage)obj;
				ResClientMessage rcm = new ResClientMessage(
						om.taskId, 1, om.key, om.value, true, true, 0);
				objBack = rcm;
				dist = om.sender;
				key = om.key;
				value = om.value;
			}
			else if (obj.getClass() == DFCForwardMessage.class)
			{
				DFCForwardMessage fm = (DFCForwardMessage)obj;
				DFCResForwardMessage rfm = new DFCResForwardMessage(fm.taskId, fm.source, fm.type, 
						fm.key, fm.value, true, fm.numHops, sender.getIndex());
				objBack = rfm;
				dist = Network.get(fm.intermid);
				key = fm.key;
				value = fm.value;
			}
			else if (obj.getClass() == DChordOperaMessage.class)
			{
				DChordOperaMessage com = (DChordOperaMessage)obj;
				DChordResMessage crm = new DChordResMessage(com.taskId, com.type, com.sender,
						com.key, com.value, true, true, com.numHops, sender.getIndex(), replicaIds[0]);
				objBack = crm;
				dist = Network.get(com.servId);
				key = com.key;
				value = com.value;
			}
			hmData.put(key, value);
			updateMaxFwdTime(Library.sendOverhead);
			EDSimulator.add(waitTimeCal(maxFwdTime), objBack, dist, par.pid);
			if (obj.getClass() == OperaMessage.class)
			{
				numLocalReqFinished++;
			}
			else
			{
				numOtherReqFinished++;
			}
			numFwdMsg++;
		}
	}
	
	public void reloadDataMsgProcess(ReloadDataMessage rdm, Node node)
	{
		numRecFromOtherMsg++;
		updateMaxFwdTime(Library.recvOverhead);
		ResReloadDataMessage rrdm = new ResReloadDataMessage(hmData);
		updateMaxFwdTime(Library.sendOverhead);
		EDSimulator.add(waitTimeCal(maxFwdTime), rrdm, Network.get(rdm.sourceId), par.pid);
		TransReqMessage trm = new TransReqMessage(new ArrayList<Object>());
		while (Library.ll[node.getIndex()].size() > 0)
		{
			trm.al.add(Library.ll[node.getIndex()].remove());
		}
		if (trm.al.size() > 0)
		{
			maxTime = CommonState.getTime();
			updateMaxFwdTime(Library.sendOverhead);
			EDSimulator.add(waitTimeCal(maxFwdTime), trm, Network.get(rdm.sourceId), par.pid);
		}
	}
	
	public void resReloadDataMessage(PeerProtocol pp, Node node, ResReloadDataMessage rrdm)
	{
		if (!isUp())
		{
			numRecFromOtherMsgFailed++;
			return;
		}
		else
		{
			maxTime = CommonState.getTime();
			maxFwdTime = CommonState.getTime();
			updateMaxFwdTime(Library.recvOverhead);
			hmData = rrdm.hmData;
			ready = true;
		}
	}
	
	public int gatherSize(int nodeId, int numClientPerServ)
	{
		int numNodeLeft = 0;
		int clientId = 0;
		int offset = 1 + numReplica;
		for (int i = 0;i < numClientPerServ;i++)
		{
			clientId = (nodeId - offset) * numClientPerServ + Library.offset + i;
			if (clientId > Network.size() - 1)
			{
				break;
			}
			if (((PeerProtocol)Network.get(clientId).getProtocol(par.pid)).
					numLocalReqFinished < numOpera)
			{
				numNodeLeft++;
			}
		}
		return numNodeLeft;
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
		while (!memList.containsKey(replicaId))
		{
			replicaId = (replicaId + 1) % numServer;
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
		HashMap<Integer, HashMap<String, String>> hmAllDataToSend 
						= new HashMap<Integer, HashMap<String, String>>();
		HashMap<String, String> hmDataToSend = hmAllData.get(dataResideId);
		if (!all)
		{
			hmAllDataToSend.put(dataResideId, hmDataToSend);
		}
		else
		{
			Iterator<Integer> it = hmAllData.keySet().iterator();
			while (it.hasNext())
			{
				int id = it.next();
				if (id != dataResideId)
				{
					hmAllDataToSend.put(id, hmAllData.get(id));
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
		if (all)
		{
			Set<Long> it = hmReplica.keySet();
			Long[] itArray = new Long[it.size()];
			it.toArray(itArray);
			ArrayList<Object> al = new ArrayList<Object>();
			for (int i = 0;i < itArray.length;i++)
			{
				long temp = itArray[i];
				ReplicaInfo ri = hmReplica.get(temp);
				Object obj = ri.obj;
				if (obj.getClass() == OperaMessage.class)
				{
					OperaMessage om = (OperaMessage)obj;
					if (Library.type == 8)
					{
						int target = hashServer(new BigInteger(om.key), numServer);
						if (target != senderId)
						{
							al.add(om);
							hmReplica.remove(temp);
						}
					}
					if (Library.type == 9)
					{
						BigInteger idKey = new BigInteger(om.key);
						BigInteger idResp = id;
						BigInteger preId = ((PeerProtocol)(predecessor.getProtocol(par.pid))).id;
						//BigInteger preId = ((PeerProtocol)pp.predecessor.getProtocol(pp.par.pid)).id;
						if (!((idKey.compareTo(idResp) <= 0 && idKey.compareTo(preId) > 0) || 
										(preId.compareTo(idResp) > 0 && (idKey.compareTo(preId) > 0 
										|| idKey.compareTo(idResp) <= 0))))
						{
							al.add(om);
							hmReplica.remove(temp);
						}
					}
				}
				else if (obj.getClass() == DFCForwardMessage.class)
				{
					DFCForwardMessage fm = (DFCForwardMessage)obj;
					if (hashServer(new BigInteger(fm.key), numServer) != senderId)
					{
						al.add(fm);
						hmReplica.remove(temp);
					}
				}
				else if (obj.getClass() == DChordOperaMessage.class)
				{
					DChordOperaMessage com = (DChordOperaMessage)obj;
					BigInteger idKey = new BigInteger(com.key);
					BigInteger idResp = id;
					BigInteger preId = ((PeerProtocol)(predecessor.getProtocol(par.pid))).id;
					//BigInteger preId = ((PeerProtocol)pp.predecessor.getProtocol(pp.par.pid)).id;
					if (!((idKey.compareTo(idResp) <= 0 && idKey.compareTo(preId) > 0) || 
									(preId.compareTo(idResp) > 0 && (idKey.compareTo(preId) > 0 
									|| idKey.compareTo(idResp) <= 0))))
					{
						al.add(com);
						hmReplica.remove(temp);
					}
				}
			}
			if (al.size() > 0)
			{
				TransReqMessage trm = new TransReqMessage(al);
				updateMaxFwdTime(Library.sendOverhead);
				endTime = maxFwdTime;
				if (updateGlobalQueue(Network.get(senderId), 
						startTime, endTime, trm, Network.get(receiveId)))
				{
					numFwdMsg++;
				}
			}
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
		else if (ithReplicaF == -1 && hmAllData.containsKey(churnNodeIdx))
		{
			hmAllData.remove(churnNodeIdx);
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