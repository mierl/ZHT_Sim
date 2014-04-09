import java.io.IOException;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class CTreeRepChurn 
{
	public Node node;
	public PeerProtocol pp;
	
	public CTreeRepChurn(Node node, PeerProtocol pp)
	{
		this.node = node;
		this.pp = pp;
	}
		
	public void gatherOperaMsgProcess(OperaMessage om)
	{
		pp.numLocalReq++;
		int actGatherSize = pp.gatherSize;
		if (Library.numAllOpera < pp.gatherSize * 10)
		{
			actGatherSize = Network.size() - Library.offset;
		}
		int gatherSize = pp.gatherSize(node.getIndex(), 1024);
		if (actGatherSize > gatherSize)
		{
			actGatherSize = gatherSize;
		}
		if (pp.al.size() >= actGatherSize)
		{
			if (!pp.alBackUp.contains(om))
			{
				pp.alBackUp.add(om);
			}
			return;
		}
		pp.al.add(om);
		pp.updateMaxFwdTime(Library.recvOverhead);
		if (pp.al.size() == actGatherSize || pp.al.size() == 
				Library.numAllOpera - Library.numOperaFinished 
				|| pp.al.size() == pp.alBackUp.size())
		{
			CTreeForwardMessage fm = new CTreeForwardMessage(node.getIndex(), pp.al);
			pp.updateMaxFwdTime(Library.sendOverhead);
			EDSimulator.add(pp.maxFwdTime - CommonState.getTime() + Library.getPackCommOverhead
					(pp.al.size()), fm, Network.get(0), pp.par.pid);
			pp.numFwdMsg++;
			pp.numFwdReq += pp.al.size();
			pp.count++;
		}
	}
	
	public void operaMsgProcess(OperaMessage om)
	{
		if (!pp.isUp())
		{
			pp.numLocalReqFailed++;
			pp.numRecFromOtherMsgFailed++;
			ResClientMessage rcm = new ResClientMessage(om.taskId, 
					om.type, om.key, om.value, false, true, 0);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
									rcm, om.sender, pp.par.pid);
			return;
		}
		if (!pp.ready)
		{
			om.queued = true;
			Library.ll[node.getIndex()].add(om);
		}
		else
		{
			long startTime = CommonState.getTime();
			long endTime = 0;
			if (node.getIndex() == 1 || pp.numReplica == 0 || om.type == 0)
			{
				ResClientMessage rcm = (ResClientMessage)pp.actOperaMsgProcess(om, node);
				pp.procToFwd();
				pp.updateMaxFwdTime(Library.sendOverhead);
				endTime = pp.maxFwdTime;
				if (node.getIndex() == 1)
				{
					if (endTime <= Library.currChurnTime || startTime >= 
							Library.currChurnTime && endTime <= Library.nextChurnTime)
					{
							EDSimulator.add(pp.waitTimeCal(endTime), rcm, om.sender, pp.par.pid);
							pp.numLocalReqFinished++;
							pp.numFwdMsg++;
					}
					else
					{
						Library.ll[node.getIndex()].add(om);
					}
				}
				else if (pp.updateGlobalQueue(node, startTime, endTime, rcm, om.sender))
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
	}
	
	public void fwdMsgProcess(CTreeForwardMessage fm)
	{
		CTreeResForwardMessage rfm = null;
		if (!pp.isUp())
		{
			rfm = new CTreeResForwardMessage(false);
			pp.numRecFromOtherMsgFailed++;
			EDSimulator.add(Library.sendOverhead + 
					Library.commOverhead, rfm, Network.get(fm.senderId), pp.par.pid);
			return;
		}
		rfm = new CTreeResForwardMessage(true);
		pp.numLocalReq += fm.al.size();
		long startTime = CommonState.getTime();
		long endTime = 0;
		pp.updateMaxFwdTime(Library.recvOverhead);
		pp.updateMaxFwdTime(Library.sendOverhead);
		endTime = pp.maxFwdTime;
		if (node.getIndex() == 0)
		{
			if (pp.updateGlobalQueue(node, startTime, endTime, rfm, Network.get(fm.senderId)))
			{
				pp.numFwdMsg++;
			}
		}
		else
		{
			EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), rfm, Network.get(fm.senderId), pp.par.pid);
		}
		pp.fwdToProc();
		for (int i = 0;i < fm.al.size();i++)
		{
			OperaMessage om = fm.al.get(i);
			operaMsgProcess(om);
		}
	}
	
	public void resFwdMsgProcess(CTreeResForwardMessage rfm)
	{
		pp.updateMaxFwdTime(Library.recvOverhead);
		if (rfm.success)
		{
			pp.al.clear();
			for (int i = 0;i < pp.alBackUp.size();i++)
			{
				gatherOperaMsgProcess(pp.alBackUp.get(i));
			}
			for (int i = 0;i < pp.al.size();i++)
			{
				pp.alBackUp.remove(pp.al.get(i));
			}
		}
		else
		{
			pp.updateMaxFwdTime(Library.sendOverhead);
			CTreeForwardMessage fm = new CTreeForwardMessage(node.getIndex(), pp.al);
			if (pp.hmTry.containsKey(pp.count))
			{
				Integer numTry = pp.hmTry.get(pp.count);
				numTry++;
				pp.hmTry.put(pp.count, numTry);
				int target = 0;
				if (numTry == pp.maxNumTry)
				{
					target++;
				}
				EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), fm, Network.get(target), pp.par.pid);
			}
			else
			{
				pp.hmTry.put(pp.count, 1);
				EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), fm, Network.get(0), pp.par.pid);
			}
		}
	}
	
	public void transReqMsgProcess(TransReqMessage trm)
	{
		if (pp.isUp())
		{
			pp.updateMaxFwdTime(Library.recvOverhead);
		}
		for (int i = 0;i < trm.al.size();i++)
		{
			OperaMessage om = (OperaMessage)trm.al.get(i);
			om.queued = true;
			if (pp.ready)
			{
				operaMsgProcess(om);
			}
			else
			{
				Library.ll[node.getIndex()].add(om);
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
		if (event.getClass() != ResClientMessage.class)
		{
			pp.numRecFromOtherMsg++;
		}
		if (event.getClass() == ChurnNoticeMessage.class)
		{
			pp.churnNotMsgProcess(node);
			return;
		}
		else if (event.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)event;
			gatherOperaMsgProcess(om);
		}
		else if (event.getClass() == CTreeForwardMessage.class)
		{
			CTreeForwardMessage fm = (CTreeForwardMessage)event;
			fwdMsgProcess(fm);
		}
		else if (event.getClass() == CTreeResForwardMessage.class)
		{
			CTreeResForwardMessage rfm = (CTreeResForwardMessage)event;
			resFwdMsgProcess(rfm);
		}
		else if (event.getClass() == ResClientMessage.class)
		{
			ResClientMessage rcm = (ResClientMessage)event;
			pp.resClientMsgProcessRepChurn(node, Library.file + "/summary_client_0_" + 
			Integer.toString(Network.size() - Library.offset) +  "_" 
			+ pp.numReplica + "_" + Library.churnInterval + ".txt", rcm);
		}
		else if (event.getClass() == ReplicaMessage.class)
		{
			ReplicaMessage rm = (ReplicaMessage)event;
			pp.replicaMsgProcess(node, rm);
		}
		else if (event.getClass() == ResReplicaMessage.class)
		{
			ResReplicaMessage rrm = (ResReplicaMessage)event;
			pp.resReplicaMsgProcessRepChurn(pp, node, rrm);
		}
		else if (event.getClass() == ReloadDataMessage.class)
		{
			ReloadDataMessage rdm = (ReloadDataMessage)event;
			pp.reloadDataMsgProcess(rdm, node);
		}
		else if (event.getClass() == ResReloadDataMessage.class)
		{
			ResReloadDataMessage rrdm = (ResReloadDataMessage)event;
			pp.resReloadDataMessage(pp, node, rrdm);
			processQueuedEvent();
		}
		else if (event.getClass() == TransReqMessage.class)
		{
			TransReqMessage trm = (TransReqMessage)event;
			transReqMsgProcess(trm);
		}
		else if (event.getClass() == ResClientMessageChurn.class)
		{
			ResClientMessageChurn rcmc = (ResClientMessageChurn)event;
			pp.resClientMsgProcessRepChurn2(node, rcmc);
		}
	}
}