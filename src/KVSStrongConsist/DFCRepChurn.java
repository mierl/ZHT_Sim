import java.io.IOException;
import java.math.*;
import java.util.Iterator;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class DFCRepChurn 
{
	public Node node;
	public PeerProtocol pp;
	
	public DFCRepChurn(Node node, PeerProtocol pp)
	{
		this.node = node;
		this.pp = pp;
	}
	
	public void fwdProcessing(long startTime, DFCForwardMessage fm, int target)
	{
		pp.updateMaxFwdTime(Library.sendOverhead);
		long endTime = pp.maxFwdTime;
		if (pp.updateGlobalQueue(node, startTime, endTime, fm, Network.get(target)))
		{
			pp.numFwdReq++;
			pp.numFwdMsg++;
		}
	}
	
	public void localProcess(OperaMessage om, long startTime)
	{
		if (pp.numReplica == 0 || om.type == 0)
		{
			ResClientMessage rcm = (ResClientMessage)pp.actOperaMsgProcess(om, node);
			pp.procToFwd();
			pp.updateMaxFwdTime(Library.sendOverhead);
			long endTime = pp.maxFwdTime;
			if (pp.updateGlobalQueue(node, startTime, endTime, rcm, om.sender))
			{
				pp.numLocalReqFinished++;
				pp.numFwdMsg++;
			}
		}
		else
		{
			pp.actOperaMsgProcess(om, node);
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
			ResClientMessageChurn rcmc = new ResClientMessageChurn(om.taskId, 
					om.type, om.key, om.value, false, true, 0, node.getIndex(), replicaId);
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
			int target = pp.hashServer(new BigInteger(om.key), pp.numServer);
			long startTime = CommonState.getTime();
			if (target == om.originId)
			{
				localProcess(om, startTime);
			}
			else
			{
				DFCForwardMessage fm = new DFCForwardMessage(om.taskId, 
						om.sender, node.getIndex(), om.type, om.key, om.value, 1, false);
				int distId = target;
				if (!pp.memList.containsKey(new Integer(target)))
				{
					distId = pp.getNextReplica(target);
				}
				if (distId == node.getIndex())
				{
					localProcess(om, startTime);
				}
				else
				{
					fwdProcessing(startTime, fm, distId);
				}
			}
		}
	}
	
	public void fwdMsgProcess(DFCForwardMessage fm)
	{
		DFCResForwardMessage rfm = null;
		if (!pp.isUp())
		{
			rfm = new DFCResForwardMessage(fm.taskId, fm.source, 
					fm.type, fm.key, fm.value, false, fm.numHops, node.getIndex());
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, rfm, 
										Network.get(fm.intermid), pp.par.pid);
			pp.numRecReqFromOtherFailed++;
			pp.numRecFromOtherMsgFailed++;
			return;
		}
		if (!fm.queued)
		{
			pp.numRecReqFromOther++;
			pp.numRecFromOtherMsg++;
			pp.updateMaxFwdTime(Library.recvOverhead);
		}
		if (!pp.ready)
		{
			fm.queued = true;
			Library.ll[node.getIndex()].add(fm);
			return;
		}
		long startTime = CommonState.getTime();
		if (pp.numReplica == 0 || fm.type == 0)
		{
			rfm = (DFCResForwardMessage)pp.actOperaMsgProcess(fm, node);
			pp.procToFwd();
			pp.updateMaxFwdTime(Library.sendOverhead);
			long endTime = pp.maxFwdTime;
			if (pp.updateGlobalQueue(node, startTime, endTime, rfm, Network.get(fm.intermid)))
			{
				pp.numOtherReqFinished++;
				pp.numFwdMsg++;
			}
		}
		else
		{
			pp.actOperaMsgProcess(fm, node);
		}
	}
	
	public void resFwdMsgProcess(DFCResForwardMessage rfm)
	{
		ResClientMessage rcm = null;
		if (!pp.isUp())
		{
			pp.numRecFromOtherMsgFailed++;
			int firstReplicaId = pp.getNextReplica(node.getIndex());
			ResClientMessageChurn rcmc = new ResClientMessageChurn(rfm.taskId, rfm.type, rfm.key, 
						rfm.value, false, false, rfm.numHops, node.getIndex(), firstReplicaId);
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
				rcm = new ResClientMessage(rfm.taskId, rfm.type, rfm.key, 
											rfm.value, rfm.success, false, rfm.numHops);
				pp.updateMaxFwdTime(Library.sendOverhead);
				endTime = pp.maxFwdTime;
				if (pp.updateGlobalQueue(node, startTime, endTime, rcm, rfm.source))
				{
					pp.numFwdMsg++;
				}
			}
			else
			{
				DFCForwardMessage fm = new DFCForwardMessage(
						rfm.taskId, rfm.source,  node.getIndex(), 
						rfm.type, rfm.key, rfm.value, rfm.numHops, false);
				int target = rfm.servId;
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
							OperaMessage om = new OperaMessage(rfm.taskId, 
							rfm.source, rfm.type, rfm.key, rfm.value, node.getIndex(), false);
							localProcess(om, startTime);
							return;
						}
					}
				}
				else
				{
					pp.hmTry.put(rfm.taskId, 1);
				}
				pp.updateMaxFwdTime(Library.sendOverhead);
				endTime = pp.maxFwdTime;
				if (pp.updateGlobalQueue(node, startTime, endTime, fm, Network.get(target)))
				{
					pp.numFwdMsg++;
					pp.numFwdReq++;
				}
			}
		}
	}
	
	public void doBroadCast(NetChurnMessage ncmForward)
	{
		Iterator<Integer> it = pp.memList.keySet().iterator();
		int i = 0;
		long startTime = 0, endTime = 0;
		while (it.hasNext())
		{
			int dist = it.next();
			if (dist != node.getIndex() && dist != ncmForward.nodeId)
			{
				startTime = pp.maxFwdTime + Library.sendOverhead * i;
				endTime = startTime + Library.sendOverhead;
				if (pp.updateGlobalQueue(node, startTime, endTime, ncmForward, Network.get(dist)))
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
			while (it.hasNext())
			{
				int id = it.next();
				pp.hmAllData.put(id, dfm.hmData.get(id));
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
				if (pp.updateGlobalQueue(node, startTime, 
						endTime, drfm, Network.get(dfm.senderId)))
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
			pp.hmAllData.remove(ddm.senderId);
		}
	}
	
	public void transReqMsgProcess(TransReqMessage trm)
	{
		if (!pp.isUp())
		{
			for (int i = 0;i < trm.al.size();i++)
			{
				Object obj = trm.al.get(i);
				if (obj.getClass() == OperaMessage.class)
				{
					OperaMessage om = (OperaMessage)obj;
					int firstReplicaId = pp.getNextReplica(node.getIndex());
					ResClientMessageChurn rcmc = new ResClientMessageChurn(om.taskId, om.type, 
							om.key, om.value, false, true, 1, node.getIndex(), firstReplicaId);
					EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
							rcmc, om.sender, pp.par.pid);
				}
				else if (obj.getClass() == DFCForwardMessage.class)
				{
					DFCForwardMessage fm = (DFCForwardMessage)obj;
					DFCResForwardMessage rfm = new DFCResForwardMessage(fm.taskId, fm.source, fm.type, 
							fm.key, fm.value, false, 2, node.getIndex());
					EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
							rfm, Network.get(fm.intermid), pp.par.pid);
				}
			}
		}
		else
		{
			pp.updateMaxFwdTime(Library.recvOverhead);
			for (int i = 0;i < trm.al.size();i++)
			{
				Object obj = trm.al.get(i);
				if (obj.getClass() == OperaMessage.class)
				{
					OperaMessage om = (OperaMessage)obj;
					om.queued = true;
					if (!pp.ready)
					{
						Library.ll[node.getIndex()].add(om);
					}
					else
					{
						operaMsgProcess(om);
					}
				}
				else if (obj.getClass() == DFCForwardMessage.class)
				{
					DFCForwardMessage fm = (DFCForwardMessage)obj;
					fm.queued = true;
					if (!pp.ready)
					{
						Library.ll[node.getIndex()].add(fm);
					}
					else
					{
						fwdMsgProcess(fm);
					}
				}
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
	
	public void processEvent(Object event)
	{
		if (event.getClass() == LogMessage.class)
		{
			try
			{
				pp.logEventProc();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			return;
		}
		Library.numAllMessage++;
		if (event.getClass() == ChurnNoticeMessage.class)
		{
			Library.numChurnMsg++;
			pp.churnNotMsgProcess(node);
			return;
		}
		if (event.getClass() == OperaMessage.class)
		{
			Library.numOperaMsg++;
			OperaMessage om = (OperaMessage)event;
			operaMsgProcess(om);
		}
		else if (event.getClass() == ResClientMessage.class)
		{
			Library.numOperaMsg++;
			ResClientMessage rcm = (ResClientMessage)event;
			pp.resClientMsgProcessRepChurn(node, Library.file + "/summary_client_0_" + 
						Integer.toString(Network.size() - Library.offset) +  "_" + 
						pp.numReplica + "_" + Library.churnInterval + ".txt", rcm);
		}
		else if (event.getClass() == ResClientMessageChurn.class)
		{
			Library.numOperaMsg++;
			ResClientMessageChurn rcmc = (ResClientMessageChurn)event;
			pp.resClientMsgProcessRepChurn2(node, rcmc);
		}
		else if (event.getClass() == DFCForwardMessage.class)
		{
			Library.numOperaMsg++;
			DFCForwardMessage fm = (DFCForwardMessage)event;
			fwdMsgProcess(fm);
		}
		else if (event.getClass() == DFCResForwardMessage.class)
		{
			Library.numOperaMsg++;
			DFCResForwardMessage rfm = (DFCResForwardMessage)event;
			resFwdMsgProcess(rfm);
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
		else if (event.getClass() == ReplicaMessage.class)
		{
			Library.numStrongConsistMsg++;
			ReplicaMessage rm = (ReplicaMessage)event;
			pp.replicaMsgProcess(node, rm);
		}
		else if (event.getClass() == ResReplicaMessage.class)
		{
			Library.numStrongConsistMsg++;
			ResReplicaMessage rrm = (ResReplicaMessage)event;
			pp.resReplicaMsgProcessRepChurn(pp, node, rrm);
		}
		else if (event.getClass() == TransReqMessage.class)
		{
			Library.numStrongConsistMsg++;
			TransReqMessage trm = (TransReqMessage)event;
			transReqMsgProcess(trm);
		}
	}
}