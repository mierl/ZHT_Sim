import java.io.IOException;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class CTree 
{
	public Node node;
	public PeerProtocol pp;
	
	public CTree(Node node, PeerProtocol pp)
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
		pp.al.add(om);
		pp.updateMaxFwdTime(Library.recvOverhead);
		if (pp.al.size() == actGatherSize)
		{
			CTreeForwardMessage fm = new CTreeForwardMessage(node.getIndex(), pp.al);
			pp.updateMaxFwdTime(Library.sendOverhead);
			EDSimulator.add(pp.maxFwdTime - CommonState.getTime() + Library.getPackCommOverhead
					(pp.al.size()), fm, Network.get(0), pp.par.pid);
			pp.numFwdMsg++;
			pp.numFwdReq += pp.al.size();
			pp.al.clear();
		}
	}
		
	public void fwdMsgProcess(CTreeForwardMessage fm)
	{
		pp.numLocalReq += fm.al.size();
		pp.updateMaxFwdTime(Library.recvOverhead);
		pp.fwdToProc();
		for (int i = 0;i < fm.al.size();i++)
		{
			OperaMessage om = fm.al.get(i);
			if (pp.numReplica == 0 || om.type == 0)
			{
				ResClientMessage rcm = (ResClientMessage)pp.actOperaMsgProcess(om, node);
				pp.procToFwd();
				pp.updateMaxFwdTime(Library.sendOverhead);
				EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), rcm, om.sender, pp.par.pid);
				pp.numLocalReqFinished++;
				pp.numFwdMsg++;
			}
			else
			{
				pp.actOperaMsgProcess(om, node);
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
		if (event.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)event;
			gatherOperaMsgProcess(om);
		}
		else if (event.getClass() == CTreeForwardMessage.class)
		{
			CTreeForwardMessage fm = (CTreeForwardMessage)event;
			fwdMsgProcess(fm);
		}
		else if (event.getClass() == ResClientMessage.class)
		{
			ResClientMessage rcm = (ResClientMessage)event;
			pp.resClientMsgProcess(node, Library.file + "/summary_client_0_" + 
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
			pp.resReplicaMsgProcess(node, rrm);
		}
	}
}