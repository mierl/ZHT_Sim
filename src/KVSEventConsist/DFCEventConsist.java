import java.util.*;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class DFCEventConsist 
{
	public Node node;
	public PeerProtocol pp;
	
	public DFCEventConsist(Node node, PeerProtocol pp)
	{
		this.node = node;
		this.pp = pp;
	}
	
	public void fwdProcessing(DFCForwardMessage dfm, int destId)
	{
		pp.updateMaxFwdTime(Library.sendOverhead);
		long startTime = CommonState.getTime();
		long endTime = pp.maxFwdTime;
		if (pp.updateGlobalQueue(node, startTime, endTime, dfm, Network.get(destId)))
		{
			pp.numFwdReq++;
			pp.numFwdMsg++;
		}
	}
	
	public void operaMsgProcess(OperaMessage om)
	{
		if (!om.queued)
		{
			pp.numLocalReq++;
		}
		if (!pp.isUp())
		{
			pp.numLocalReqFailed++;
			pp.numRecFromOtherMsgFailed++;
			int replicaId = pp.getNextReplica(node.getIndex());
			ResClientMessageChurn rcmc = new ResClientMessageChurn(om.qk.taskId, om.type, 
					om.key, om.value, false, true, 0, node.getIndex(), replicaId);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
									rcmc, om.sender, pp.par.pid);
		}
		else
		{
			if (!om.queued)
			{
				pp.numRecFromOtherMsg++;
				pp.updateMaxFwdTime(Library.recvOverhead);
			}
			if (!pp.ready)
			{
				om.queued = true;
				Library.ll[node.getIndex()].add(om);
				return;
			}
			
			int[] prefList = pp.prefList(om.key);
			for (int i = 0;i <= pp.numReplica;i++)
			{
				if (prefList[i] == node.getIndex())
				{
					pp.actOperaMsgProcessEvenConsit(om, node);
					return;
				}
			}
			int destId = prefList[(int)(Math.random() * (pp.numReplica + 1))];
			DFCForwardMessage dfm = new DFCForwardMessage(new QuorumKey(om.qk.taskId, 1),
					om.sender, node.getIndex(), om.type, om.key, om.value, 1, false);
			fwdProcessing(dfm, destId);
		}
	}
	
	public void fwdMsgProcess(DFCForwardMessage dfm)
	{
		DFCResForwardMessage rfm = null;
		if (!pp.isUp())
		{
			rfm = new DFCResForwardMessage(dfm.qk.taskId, dfm.source, dfm.type, dfm.key, 
										dfm.value, false, dfm.numHops, node.getIndex());
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, rfm, 
										Network.get(dfm.intermid), pp.par.pid);
			pp.numRecReqFromOtherFailed++;
			pp.numRecFromOtherMsgFailed++;
		}
		else
		{
			if (!dfm.queued)
			{
				pp.numRecReqFromOther++;
				pp.numRecFromOtherMsg++;
				pp.updateMaxFwdTime(Library.recvOverhead);
			}
			if (!pp.ready)
			{
				dfm.queued = true;
				Library.ll[node.getIndex()].add(dfm);
				return;
			}
			pp.actOperaMsgProcessEvenConsit(dfm, node);
		}
	}
	
	public void resFwdMsgProcess(DFCResForwardMessage rfm)
	{
		ResClientMessageChurn rcmc = null;
		int firstReplicaId = pp.getNextReplica(node.getIndex());
		if (!pp.isUp())
		{
			pp.numRecFromOtherMsgFailed++;
			rcmc = new ResClientMessageChurn(rfm.taskId, rfm.type, rfm.key, rfm.value, 
							false, false, rfm.numHops, node.getIndex(), firstReplicaId);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
					rcmc, rfm.source, pp.par.pid);
		}
		else
		{
			long startTime = CommonState.getTime();
			long endTime = 0;
			pp.numRecFromOtherMsg++;
			pp.updateMaxFwdTime(Library.recvOverhead);
			if (rfm.success)
			{
				rcmc = new ResClientMessageChurn(rfm.taskId, rfm.type, rfm.key, rfm.value, 
						rfm.success, false, rfm.numHops, node.getIndex(), firstReplicaId);
				pp.updateMaxFwdTime(Library.sendOverhead);
				endTime = pp.maxFwdTime;
				if (pp.updateGlobalQueue(node, startTime, endTime, rcmc, rfm.source))
				{
					pp.numFwdMsg++;
				}
			}
			else
			{
				DFCForwardMessage dfm = new DFCForwardMessage(new QuorumKey(rfm.taskId,1), 
							rfm.source,  node.getIndex(), rfm.type, rfm.key, 
							rfm.value, rfm.numHops, false);
				int target = rfm.servId, time = 0;
				if (pp.hmTry.containsKey(rfm.taskId))
				{
					Integer numTry = pp.hmTry.get(rfm.taskId);
					numTry++;
					pp.hmTry.put(rfm.taskId, numTry);
					if (numTry == pp.maxNumTry)
					{
						target = pp.getNextReplica(target);
						pp.hmTry.remove(rfm.taskId);
						if (target == node.getIndex())
						{
							OperaMessage om = new OperaMessage(new QuorumKey(rfm.taskId,1), 
							rfm.source, rfm.type, rfm.key, rfm.value, node.getIndex(), false);
							pp.actOperaMsgProcessEvenConsit(om, node);
							return;
						}	
					}
					else
					{
						time = numTry + 1;
					}
				}
				else
				{
					pp.hmTry.put(rfm.taskId, 1);
					time = 2;
				}
				dfm.qk.time = time;
				pp.updateMaxFwdTime(Library.sendOverhead);
				endTime = pp.maxFwdTime;
				if (pp.updateGlobalQueue(node, startTime, endTime, dfm, Network.get(target)))
				{
					pp.numFwdMsg++;
					pp.numFwdReq++;
				}
			}
		}
	}
	
	public void doBroadCast(NetChurnMessage ncm)
	{
		Iterator<Integer> it = pp.memList.keySet().iterator();
		int i = 0;
		long startTime = CommonState.getTime(), endTime = 0;
		while (it.hasNext())
		{
			int dest = it.next();
			if (dest != node.getIndex() && dest != ncm.nodeId)
			{
				endTime = pp.maxFwdTime + Library.sendOverhead * (i + 1);
				if (pp.updateGlobalQueue(node, startTime, endTime, ncm, Network.get(dest)))
				{
					pp.numFwdMsg++;
				}
				i++;
			}
		}
		pp.updateMaxFwdTime(Library.sendOverhead * i);
	}
	
	public void netChurnMsgProcess(NetChurnMessage ncm)
	{
		MemMessage mm = null;
		if (!pp.isUp())
		{
			pp.numRecFromOtherMsgFailed++;
			if (ncm.notify)
			{
				mm = new MemMessage(ncm.type, null, ncm.nodeId, false);
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
										mm, Network.get(ncm.nodeId), pp.par.pid);
			}
			return;
		}
		pp.numRecFromOtherMsg++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		long startTime = CommonState.getTime();
		long endTime = 0;
		long extra = 0;
		pp.fwdToProc();
		pp.updateMaxTime(Library.procTime);
		if (ncm.notify)
		{
			//pp.procToFwd();
			pp.updateMaxFwdTime(Library.sendOverhead);
			if (ncm.type)
			{
				mm = new MemMessage(ncm.type, pp.memList, ncm.nodeId, true);
				extra = Library.getPackCommOverhead(pp.memList.size());
				pp.memList.put(ncm.nodeId, null);
			}
			else
			{
				pp.memList.remove(ncm.nodeId);
				mm = new MemMessage(ncm.type, null, ncm.nodeId, true);
				extra = Library.commOverhead;
			}
			endTime = pp.maxFwdTime;
			long wait = pp.maxFwdTime - CommonState.getTime() + extra;
			if (endTime <= Library.currChurnTime || startTime >= 
				Library.currChurnTime && endTime <= Library.nextChurnTime)
			{
				EDSimulator.add(wait, mm, Network.get(ncm.nodeId), pp.par.pid);
				pp.numFwdMsg++;
			}
			else
			{
				Library.waitQueue.get(node.getIndex()).add(new WaitingEvent(ncm.nodeId, 
										startTime, endTime, extra, mm));
			}
			NetChurnMessage ncmForward = new NetChurnMessage(ncm.type, ncm.nodeId, false);
			doBroadCast(ncmForward);
		}
		else
		{
			if (ncm.type)
			{
				pp.memList.put(ncm.nodeId, null);
			}
			else
			{
				pp.memList.remove(new Integer(ncm.nodeId));
			}
		}
		pp.fixReplica(node.getIndex(), ncm.nodeId, ncm.type);
	}
	
	public void memMsgProcess(MemMessage mm)
	{
		if (!mm.success)
		{
			if (Library.memList.size() > 0)
			{
				Library.dfcChurn(mm.type, node, pp.par.pid);
			}
		}
		else 
		{
			if (pp.isUp())
			{
				Iterator<Integer> it = mm.memList.keySet().iterator();
				int i = 0;
				while (it.hasNext())
				{
					pp.memList.put(it.next(), null);
					i++;
				}
				pp.updateMaxTime(Library.procTime * i);
			}
		}
	}
	
	public void processQueuedEvent()
	{
		while (Library.ll[node.getIndex()].size() > 0)
		{
			Object obj = Library.ll[node.getIndex()].remove();
			if (obj.getClass() == OperaMessage.class)
			{
				OperaMessage om = (OperaMessage)obj;
				operaMsgProcess(om);
			}
			else if (obj.getClass() == DFCForwardMessage.class)
			{
				DFCForwardMessage fm = (DFCForwardMessage)obj;
				fwdMsgProcess(fm);
			}
		}
	}
	
	public void dataFwdMsgProcess(DataForwardMessage dfm)
	{
		DataResForwardMessage drfm = null;
		if (!pp.isUp())
		{
			pp.numRecFromOtherMsgFailed++;
			if (!dfm.type)
			{
				drfm = new DataResForwardMessage(node.getIndex(), false, pp.replicaIds);
				EDSimulator.add(Library.sendOverhead + Library.commOverhead,
						drfm, Network.get(dfm.senderId), pp.par.pid);
			}
		}
		else
		{
			pp.updateMaxFwdTime(Library.recvOverhead);
			long startTime = CommonState.getTime();
			long endTime = 0;
			Iterator<Integer> it = dfm.hmData.keySet().iterator();
			int id = 0;
			while (it.hasNext())
			{
				id = it.next();
				pp.versionedData.put(id, dfm.hmData.get(id));
			}
			if (dfm.type)
			{
				pp.replicaIds[0] = dfm.senderId;
				for (int i = 1;i < pp.numReplica;i++)
				{
					pp.replicaIds[i] = dfm.myReplicaIds[i - 1];
				}
				pp.ready = true;
				pp.maxTime = CommonState.getTime();
				pp.maxFwdTime = CommonState.getTime();
				processQueuedEvent();
			}
			else
			{
				drfm = new DataResForwardMessage(node.getIndex(), true, pp.replicaIds);
				pp.updateMaxFwdTime(Library.sendOverhead);
				endTime = pp.maxFwdTime;
				if (pp.updateGlobalQueue(node, startTime, endTime,
									drfm, Network.get(dfm.senderId)))
				{
					pp.numFwdMsg++;
				}
			}
		}
	}
	
	public void dataResFwdMsgProcess(DataResForwardMessage drfm)
	{
		if (!pp.isUp())
		{
			pp.numRecFromOtherMsgFailed++;
		}
		else
		{
			pp.updateMaxFwdTime(Library.recvOverhead);
			if (drfm.success)
			{
				pp.replicaIds[pp.numReplica - 1] = drfm.senderId;
			}
			else
			{
				int distId = (drfm.senderId + 1) % pp.numServer;
				while (!pp.memList.containsKey((distId)))
				{
					distId = (distId + 1) % pp.numServer;
				}
				if (node.getIndex() != distId)
				{
					pp.updateMaxFwdTime(Library.sendOverhead);
					pp.sendData(node.getIndex(), node.getIndex(), distId, false);
				}
			}
		}
	}
	
	public void dataDelMsgProcess(DataDeleteMessage ddm)
	{
		if (!pp.isUp())
		{
			pp.numRecFromOtherMsgFailed++;
		}
		else
		{
			pp.updateMaxFwdTime(Library.recvOverhead);
			pp.versionedData.remove(ddm.senderId);
		}
	}
	
	public void processEvent(Object event)
	{
		if (event.getClass() == LogMessage.class)
		{
			pp.logEventProc();
			return;
		}
		Library.numAllMessage++;
		if (event.getClass() == ChurnNoticeMessage.class)
		{
			Library.numChurnMsg++;
			pp.churnNotMsgProcess(node);
		}
		else if (event.getClass() == OperaMessage.class)
		{
			Library.numOperaMsg++;
			OperaMessage om = (OperaMessage)event;
			operaMsgProcess(om);
		}
		else if (event.getClass() == DFCForwardMessage.class)
		{
			Library.numOperaMsg++;
			DFCForwardMessage dfm = (DFCForwardMessage)event;
			fwdMsgProcess(dfm);
		}
		else if (event.getClass() == DFCResForwardMessage.class)
		{
			Library.numOperaMsg++;
			DFCResForwardMessage rfm = (DFCResForwardMessage)event;
			resFwdMsgProcess(rfm);
		}
		else if (event.getClass() == QuorumMessage.class)
		{
			Library.numEvenConsistMsg++;
			QuorumMessage qm = (QuorumMessage)event;
			pp.quoMsgProcess(node, qm);
		}
		else if (event.getClass() == QuorumResMessage.class)
		{
			Library.numEvenConsistMsg++;
			QuorumResMessage qrm = (QuorumResMessage)event;
			pp.quoResMsgProcess(node, qrm);
		}
		else if (event.getClass() == NetChurnMessage.class)
		{
			Library.numChurnMsg++;
			NetChurnMessage ncm = (NetChurnMessage)event;
			netChurnMsgProcess(ncm);
		}
		else if (event.getClass() == MemMessage.class)
		{
			Library.numChurnMsg++;
			MemMessage mm = (MemMessage)event;
			memMsgProcess(mm);
		}
		else if (event.getClass() == DataForwardMessage.class)
		{
			Library.numChurnMsg++;
			DataForwardMessage dfm = (DataForwardMessage)event;
			dataFwdMsgProcess(dfm);
		}
		else if (event.getClass() == DataResForwardMessage.class)
		{
			Library.numChurnMsg++;
			DataResForwardMessage drfm = (DataResForwardMessage)event;
			dataResFwdMsgProcess(drfm);
		}
		else if (event.getClass() == DataDeleteMessage.class)
		{
			Library.numChurnMsg++;
			DataDeleteMessage ddm = (DataDeleteMessage)event;
			dataDelMsgProcess(ddm);
		}
		else if (event.getClass() == ResClientMessageChurn.class)
		{
			Library.numOperaMsg++;
			ResClientMessageChurn rcmc = (ResClientMessageChurn)event;
			pp.resClientMsgProcessRepChurn(node, Library.file + "/summary_client_0_" + 
					Integer.toString(Network.size() - Library.offset) + "_" + pp.numReplica + "_" + 
					Library.churnInterval + ".txt", rcmc);
		}
	}
}