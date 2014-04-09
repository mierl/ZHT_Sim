import java.io.IOException;
import java.math.*;

import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class DFC 
{
	public Node node;
	public PeerProtocol pp;
	
	public DFC(Node node, PeerProtocol pp)
	{
		this.node = node;
		this.pp = pp;
	}

	public void operaMsgProcess(OperaMessage om)
	{
		pp.numLocalReq++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		int target = pp.hashServer(new BigInteger(om.key), pp.numServer);
		if (target == node.getIndex())
		{
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
		else
		{
			DFCForwardMessage fm = new DFCForwardMessage(om.taskId, om.sender, 
							node.getIndex(), om.type, om.key, om.value, 1, false);
			pp.numFwdMsg++;
			pp.numFwdReq++;
			pp.updateMaxFwdTime(Library.sendOverhead);
			EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), fm, Network.get(target), pp.par.pid);
		}
	}
	
	public void fwdMsgProcess(DFCForwardMessage fm)
	{
		pp.numRecReqFromOther++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		if (pp.numReplica == 0 || fm.type == 0)
		{
			DFCResForwardMessage rfm = (DFCResForwardMessage)pp.actOperaMsgProcess(fm, node);
			pp.procToFwd();
			pp.updateMaxFwdTime(Library.sendOverhead);
			EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), rfm, 
												Network.get(fm.intermid), pp.par.pid);
			pp.numOtherReqFinished++;
			pp.numFwdMsg++;
		}
		else
		{
			pp.actOperaMsgProcess(fm, node);
		}
	}
	
	public void resFwdMsgProcess(DFCResForwardMessage rfm)
	{
		pp.updateMaxFwdTime(Library.recvOverhead);
		ResClientMessage rom = new ResClientMessage(rfm.taskId, rfm.type, 
										rfm.key, rfm.value, true, true, rfm.numHops);
		pp.numFwdMsg++;
		pp.updateMaxFwdTime(Library.sendOverhead);
		EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), rom, rfm.source, pp.par.pid);
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
			operaMsgProcess(om);
		}
		else if (event.getClass() == DFCForwardMessage.class)
		{
			DFCForwardMessage fm = (DFCForwardMessage)event;
			fwdMsgProcess(fm);
		}
		else if (event.getClass() == DFCResForwardMessage.class)
		{
			DFCResForwardMessage rfm = (DFCResForwardMessage)event;
			resFwdMsgProcess(rfm);
		}
		else if (event.getClass() == ResClientMessage.class)
		{
			ResClientMessage rcm = (ResClientMessage)event;
			pp.resClientMsgProcess(node, Library.file + "/summary_client_0_" + 
					Integer.toString(Network.size() - Library.offset) +  "_" + 
					pp.numReplica + "_" + Library.churnInterval + ".txt", rcm);
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