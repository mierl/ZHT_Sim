import java.io.IOException;
import java.math.*;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class DChordRep 
{
	public Node node;
	public PeerProtocol pp;
	
	public DChordRep(Node node, PeerProtocol pp)
	{
		this.node = node;
		this.pp = pp;
	}
	
	public void actOperaMsgProcess(Object actEvent)
	{
		String key = null;
		String value = null;
		int operaType = 0;
		long taskId = 0;
		ResClientMessage rcm = null;
		DChordResMessage rm = null;
		if (actEvent.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)actEvent;
			operaType = om.type;
			key = om.key;
			value = om.value;
			taskId = om.taskId;
		}
		else if (actEvent.getClass() == DChordOperaMessage.class)
		{
			DChordOperaMessage com = (DChordOperaMessage)actEvent;
			operaType = com.type;
			key = com.key;
			value = com.value;
			taskId = com.taskId;
		}
		if (operaType == 0)
		{
			value = pp.hmData.get(key);
		}
		else
		{
			pp.hmData.put(key, value);
		}
		pp.updateMaxTime(Library.procTime + Library.sendOverhead);
		if (actEvent.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)actEvent;
			rcm = new ResClientMessage(om.taskId, om.type, om.key, value, true, true, 0);
			EDSimulator.add(pp.maxTime - CommonState.getTime() + Library.commOverhead + 
					Library.recvOverhead, rcm, om.sender, pp.par.pid);
		}
		else if (actEvent.getClass() == DChordOperaMessage.class)
		{
			DChordOperaMessage com = (DChordOperaMessage)actEvent;
			rm = new DChordResMessage(com.taskId, com.type, com.sender, com.key, value,
											true, true, com.numHops, node.getIndex(), -1);
			EDSimulator.add(pp.maxTime - CommonState.getTime() + Library.commOverhead + 
					Library.recvOverhead, rm, Network.get(com.servId), pp.par.pid);
		}
		TaskDetail td = Library.taskHM.get(taskId);
		pp.updateTask(td);
		pp.numFwdMsg++;
	}
	
	public void rangeJudSuccProc(DChordForwardMessage ofm)
	{
		if (ofm.numHops == 1)
		{
			DChordOperaMessage com = new DChordOperaMessage(ofm.taskId, ofm.type, 
					ofm.sender, ofm.key, ofm.value, node.getIndex(), ofm.numHops, false);
			pp.numFwdMsg++;
			pp.numFwdReq++;
			EDSimulator.add(Library.allCommOverhead, com, pp.fingerTable[0].node, pp.par.pid);
		}
		else
		{
			DChordResForwardMessage orm = new DChordResForwardMessage(ofm.taskId, ofm.messageId, ofm.type, 
								ofm.key, ofm.value, ofm.numHops, pp.fingerTable[0].node, true);
			pp.numFwdMsg++;
			EDSimulator.add(Library.allCommOverhead, orm, ofm.sender, pp.par.pid);
		}
	}
	
	public void rangeJudFailProc(DChordForwardMessage ofm, BigInteger idKey)
	{
		Node nextNode = pp.closetPreNode(node, idKey);
		DChordForwardMessage ofmNew = new DChordForwardMessage(ofm.taskId, 
					ofm.messageId, ofm.type, node, ofm.key, ofm.value,  ofm.numHops);
		pp.numFwdMsg++;
		EDSimulator.add(Library.allCommOverhead, ofmNew, nextNode, pp.par.pid);
	}
	
	public void operaMsgProcess(OperaMessage om)
	{
		pp.numLocalReq++;
		BigInteger idKey = new BigInteger(om.key);
		BigInteger preId = ((PeerProtocol)pp.predecessor.getProtocol(pp.par.pid)).id;
		
		if (pp.numServer == 1 || ((idKey.compareTo(pp.id) <= 0 && idKey.compareTo(preId) > 0) 
				|| (preId.compareTo(pp.id) > 0 && (idKey.compareTo(preId) > 0 
				|| idKey.compareTo(pp.id) <= 0))))
		{
			actOperaMsgProcess(om);
			pp.numLocalReqFinished++;
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
		if (upStreamId >= pp.numServer)
		{
			DChordOperaMessage om = new DChordOperaMessage(orm.taskId, orm.type, 
					Network.get(upStreamId), orm.key, orm.value, node.getIndex(), orm.numHops + 1, false);
			pp.numFwdMsg++;
			pp.numFwdReq++;
			EDSimulator.add(Library.allCommOverhead, om, orm.target, pp.par.pid); 
		}
		else
		{
			pp.numFwdMsg++;
			EDSimulator.add(Library.allCommOverhead, orm, Network.get(upStreamId), pp.par.pid);
		}
	}
	
	public void chordOperaMsgProcess(DChordOperaMessage com)
	{
		pp.numRecReqFromOther++;
		actOperaMsgProcess(com);
		pp.numOtherReqFinished++;
	}
	
	public void recMsgProcess(DChordResMessage rm)
	{
		ResClientMessage rcm = new ResClientMessage(rm.taskId, rm.type, 
				rm.key, rm.value, true, false, rm.numHops);
		pp.numFwdMsg++;
		EDSimulator.add(Library.allCommOverhead, rcm, rm.sender, pp.par.pid);
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
					Integer.toString(Network.size() - pp.numServer) + ".txt", rcm);
		}
	}
}
