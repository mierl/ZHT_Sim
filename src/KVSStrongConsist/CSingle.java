import java.io.IOException;

import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class CSingle 
{
	public Node node;
	public PeerProtocol pp;
	
	public CSingle(Node node, PeerProtocol pp)
	{
		this.node = node;
		this.pp = pp;
	}
	
	public void operaMsgProcess(OperaMessage om)
	{
		pp.numLocalReq++;
		pp.updateMaxFwdTime(Library.recvOverhead);
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
		else if (event.getClass() == ResClientMessage.class)
		{
			ResClientMessage rcm = (ResClientMessage)event;
			String path = Library.file + "/summary_client_0_" + Integer.toString(Network.size() - 
					Library.offset) +  "_" + pp.numReplica + "_" + Library.churnInterval + ".txt";
			pp.resClientMsgProcess(node, path, rcm);
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