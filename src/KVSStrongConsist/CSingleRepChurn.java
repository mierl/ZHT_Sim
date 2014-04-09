import java.io.IOException;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class CSingleRepChurn 
{
	public Node node;
	public PeerProtocol pp;
	
	public CSingleRepChurn(Node node, PeerProtocol pp)
	{
		this.pp = pp;
		this.node = node;
	}
	
	public void operaMsgProcess(OperaMessage om)
	{
		ResClientMessage rcm = null;
		if (!pp.isUp())
		{
			pp.numLocalReqFailed++;
			pp.numRecFromOtherMsgFailed++;
			rcm = new ResClientMessage(om.taskId, 
					om.type, om.key, om.value, false, true, 0);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
									rcm, om.sender, pp.par.pid);
			return;
		}
		pp.numLocalReq++;
		if (!om.queued)
		{
			pp.updateMaxFwdTime(Library.recvOverhead);
		}
		if (!pp.ready)
		{
			om.queued = true;
			Library.ll[node.getIndex()].add(om);
			return;
		}
		long startTime = CommonState.getTime();
		long endTime = 0;
		if (node.getIndex() >= pp.numServer || pp.numReplica == 0 || om.type == 0)
		{
			rcm = (ResClientMessage)pp.actOperaMsgProcess(om, node);
			pp.procToFwd();
			pp.updateMaxFwdTime(Library.sendOverhead);
			endTime = pp.maxFwdTime;
			if (node.getIndex() >= pp.numServer)
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
			operaMsgProcess(om);
		}
		else if (event.getClass() == ResClientMessage.class)
		{
			ResClientMessage rcm = (ResClientMessage)event;
			String path = Library.file + "/summary_client_0_" + Integer.toString(Network.size() - 
					Library.offset) +  "_" + pp.numReplica + "_" + Library.churnInterval + ".txt";
			pp.resClientMsgProcessRepChurn(node, path, rcm);
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
	}
}