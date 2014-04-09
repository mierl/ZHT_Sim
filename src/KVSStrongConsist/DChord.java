import java.io.IOException;
import java.math.*;

import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class DChord 
{
	public Node node;
	public PeerProtocol pp;
	
	public DChord(Node node, PeerProtocol pp)
	{
		this.node = node;
		this.pp = pp;
	}
	
	public void rangeJudSuccProc(DChordForwardMessage ofm)
	{
		pp.updateMaxFwdTime(Library.sendOverhead);
		if (ofm.numHops == 1)
		{
			DChordOperaMessage com = new DChordOperaMessage(ofm.taskId, ofm.type, 
					ofm.sender, ofm.key, ofm.value, node.getIndex(), ofm.numHops, false);
			pp.numFwdMsg++;
			pp.numFwdReq++;
			EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), 
											com, pp.fingerTable[0].node, pp.par.pid);
		}
		else
		{
			DChordResForwardMessage orm = new DChordResForwardMessage(
					ofm.taskId, ofm.messageId, ofm.type, ofm.key, ofm.value, 
					ofm.numHops, pp.fingerTable[0].node, true);
			pp.numFwdMsg++;
			EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), orm, ofm.sender, pp.par.pid);
		}
	}
	
	public void rangeJudFailProc(DChordForwardMessage ofm, BigInteger idKey)
	{
		Node nextNode = pp.closetPreNode(node, idKey);
		pp.updateMaxFwdTime(Library.sendOverhead);
		DChordForwardMessage ofmNew = new DChordForwardMessage(ofm.taskId, 
					ofm.messageId, ofm.type, node, ofm.key, ofm.value,  ofm.numHops);
		pp.numFwdMsg++;
		EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), ofmNew, nextNode, pp.par.pid);
	}
	
	public void operaMsgProcess(OperaMessage om)
	{
		pp.numLocalReq++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		BigInteger idKey = new BigInteger(om.key);
		BigInteger preId = ((PeerProtocol)pp.predecessor.getProtocol(pp.par.pid)).id;
		if (pp.numServer == 1 || ((idKey.compareTo(pp.id) <= 0 && idKey.compareTo(preId) > 0) 
				|| (preId.compareTo(pp.id) > 0 && (idKey.compareTo(preId) > 0 
				|| idKey.compareTo(pp.id) <= 0))))
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
			pp.messCount++;
			String messageId = pp.id.toString() + " " + Long.toString(pp.messCount);
			DChordForwardMessage ofm = new DChordForwardMessage(om.taskId, messageId, 
												om.type, om.sender, om.key, om.value, 1);
			pp.hmUpStreams.put(ofm.messageId, om.sender.getIndex());
			if (pp.rangeJudge(idKey, pp.id, ((PeerProtocol)
									pp.fingerTable[0].node.getProtocol(pp.par.pid)).id, true))
			{
				rangeJudSuccProc(ofm);
			}
			else
			{
				rangeJudFailProc(ofm, idKey);
			}
		}
	}
	
	public void operaFwdMsgProcess(DChordForwardMessage ofm)
	{
		ofm.numHops++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		pp.hmUpStreams.put(ofm.messageId, ofm.sender.getIndex());
		BigInteger idKey = new BigInteger(ofm.key);
		if (pp.rangeJudge(idKey, pp.id, ((PeerProtocol)
				pp.fingerTable[0].node.getProtocol(pp.par.pid)).id, true))
		{
			rangeJudSuccProc(ofm);
		}
		else
		{
			rangeJudFailProc(ofm, idKey);
		}
	}
	
	public void operaResMsgProcess(DChordResForwardMessage orm)
	{
		int upStreamId = pp.hmUpStreams.get(orm.messageId);
		pp.updateMaxFwdTime(Library.recvOverhead);
		if (upStreamId >= pp.numServer)
		{
			DChordOperaMessage om = new DChordOperaMessage(orm.taskId, orm.type, 
					Network.get(upStreamId), orm.key, orm.value, node.getIndex(), orm.numHops + 1, false);
			pp.numFwdMsg++;
			pp.numFwdReq++;
			pp.updateMaxFwdTime(Library.sendOverhead);
			EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), om, orm.target, pp.par.pid); 
		}
		else
		{
			pp.numFwdMsg++;
			pp.updateMaxFwdTime(Library.sendOverhead);
			EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), orm, 
													Network.get(upStreamId), pp.par.pid);
		}
	}
	
	public void chordOperaMsgProcess(DChordOperaMessage com)
	{
		pp.numRecReqFromOther++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		if (pp.numReplica == 0 || com.type == 0)
		{
			DChordResMessage crm = (DChordResMessage)pp.actOperaMsgProcess(com, node);
			pp.procToFwd();
			pp.updateMaxFwdTime(Library.sendOverhead);
			EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), 
									crm, Network.get(com.servId), pp.par.pid);
			pp.numOtherReqFinished++;
			pp.numFwdMsg++;
		}
		else
		{
			pp.actOperaMsgProcess(com, node);
		}
	}
	
	public void recMsgProcess(DChordResMessage rm)
	{
		pp.updateMaxFwdTime(Library.recvOverhead);
		ResClientMessage rcm = new ResClientMessage(rm.taskId, rm.type, 
				rm.key, rm.value, true, false, rm.numHops);
		pp.numFwdMsg++;
		pp.updateMaxFwdTime(Library.sendOverhead);
		EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), rcm, rm.sender, pp.par.pid);
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
		else if (event.getClass() == DChordForwardMessage.class)
		{
			DChordForwardMessage ofm = (DChordForwardMessage)event;
			operaFwdMsgProcess(ofm);
		}
		else if (event.getClass() == DChordResForwardMessage.class)
		{
			DChordResForwardMessage orm = (DChordResForwardMessage)event;
			operaResMsgProcess(orm);
		}
		else if (event.getClass() == DChordOperaMessage.class)
		{
			DChordOperaMessage com = (DChordOperaMessage)event;
			chordOperaMsgProcess(com);
		}
		else if (event.getClass() == DChordResMessage.class)
		{
			DChordResMessage rm = (DChordResMessage)event;
			recMsgProcess(rm);
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