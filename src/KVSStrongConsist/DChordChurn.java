import java.io.IOException;
import java.math.*;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class DChordChurn 
{
	public Node node;
	public PeerProtocol pp;
	
	public DChordChurn(Node node, PeerProtocol pp)
	{
		this.node = node;
		this.pp = pp;
	}
	
	public void operaNodeFail(Object failEvent)
	{
		pp.numRecFromOtherMsgFailed++;
		ResClientMessage rcm = null;
		DChordResForwardMessage rfm = null;
		if (failEvent.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)failEvent;
			rcm = new ResClientMessage(om.taskId, om.type, om.key, om.value, false, true, 0);
			EDSimulator.add(Library.sendOverhead + 
					Library.commOverhead, rcm, om.sender, pp.par.pid);
			pp.numLocalReqFailed++;
			TaskDetail td = Library.taskHM.get(om.taskId);
			td.taskQueuedTime = CommonState.getTime();
			td.taskEndTime = CommonState.getTime();
		}
		else if (failEvent.getClass() == DChordForwardMessage.class)
		{
			DChordForwardMessage fm = ( DChordForwardMessage)failEvent;
			rfm = new DChordResForwardMessage(fm.taskId, fm.messageId, 
									fm.type, fm.key, fm.value, fm.numHops, null, false);
			EDSimulator.add(Library.sendOverhead + 
					Library.commOverhead, rfm, fm.sender, pp.par.pid);
		}
	}
	
	public void sucHandle(DChordForwardMessage fm, long startTime)
	{
		ResClientMessage rcm = null;
		DChordOperaMessage com = null; 
		if (!pp.fingerTable[0].alive)
		{
			rcm = new ResClientMessage(fm.taskId, fm.type, fm.key, fm.value, false, true, fm.numHops);
			pp.numLocalReqFailed++;
		}
		else
		{
			com = new DChordOperaMessage(fm.taskId, fm.type, fm.sender, fm.key, 
													fm.value, node.getIndex(), fm.numHops + 1, false);
		}
		pp.updateMaxFwdTime(Library.sendOverhead);
		long endTime = pp.maxFwdTime;
		if (!pp.fingerTable[0].alive)
		{
			if (pp.updateGlobalQueue(node, startTime, endTime, rcm, fm.sender))
			{
				pp.numFwdMsg++;
			}
		}
		else
		{
			if (pp.updateGlobalQueue(node, startTime, endTime, com, pp.fingerTable[0].node))
			{
				pp.numFwdMsg++;
				pp.numFwdReq++;
			}
		}
	}
	
	public void nonSucHandle(DChordForwardMessage fm, long startTime)
	{
		pp.updateMaxFwdTime(Library.sendOverhead);
		DChordResForwardMessage orm = new DChordResForwardMessage(fm.taskId, fm.messageId, 
				fm.type, fm.key, fm.value, fm.numHops, pp.fingerTable[0].node, true);
		long endTime = pp.maxFwdTime;
		if (pp.updateGlobalQueue(node, startTime, endTime, orm, fm.sender))
		{
			pp.numFwdMsg++;
		}
	}
	
	public void rangeJudSuccProc(DChordForwardMessage fm, long startTime)
	{
		if (fm.numHops == 1)
		{
			sucHandle(fm, startTime);
		}
		else
		{
			nonSucHandle(fm, startTime);
		}
	}
	
	public void rangeJudFailProc(DChordForwardMessage fm, BigInteger idKey, long startTime)
	{
		Node nextNode = pp.closetPreNode(node, idKey);
		DChordForwardMessage fmNew = null;
		DChordResForwardMessage rfm = null;
		ResClientMessage rcm = null;
		if (nextNode == null)
		{
			if (fm.numHops > 1)
			{
				rfm = new DChordResForwardMessage(fm.taskId, fm.messageId, fm.type, 
					fm.key, fm.value, fm.numHops, null, false);
			}
			else
			{
				rcm = new ResClientMessage(fm.taskId, fm.type, 
						fm.key, fm.value, false, true, fm.numHops);
				TaskDetail td = Library.taskHM.get(fm.taskId);
				td.taskQueuedTime = CommonState.getTime();
				td.taskEndTime = CommonState.getTime();
			}
		}
		else
		{ 
			fmNew = new DChordForwardMessage(fm.taskId, fm.messageId, fm.type, node,
									fm.key, fm.value, fm.numHops);
		}
		pp.updateMaxFwdTime(Library.sendOverhead);
		long endTime = pp.maxFwdTime;
		if (nextNode == null)
		{
			if (fm.numHops > 1)
			{
				if (pp.updateGlobalQueue(node, startTime, endTime, rfm, fm.sender))
				{
					pp.numFwdMsg++;
				}
			}
			else
			{
				if (pp.updateGlobalQueue(node, startTime, endTime, rcm, fm.sender))
				{
					pp.numFwdMsg++;
				}
			}
		}
		else
		{
			if (pp.updateGlobalQueue(node, startTime, endTime, fmNew, nextNode))
			{
				pp.numFwdMsg++;
			}
		}
	}
	
	public void operaMsgProcess(OperaMessage om)
	{
		pp.numLocalReq++;
		if (!pp.isUp())
		{
			operaNodeFail(om);
			return;
		}
		pp.numRecFromOtherMsg++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		BigInteger idKey = new BigInteger(om.key);
		BigInteger preId = ((PeerProtocol)pp.predecessor.getProtocol(pp.par.pid)).id;
		long startTime = CommonState.getTime();
		if (pp.numServer == 1 || ((idKey.compareTo(pp.id) <= 0 && idKey.compareTo(preId) > 0) || 
						(preId.compareTo(pp.id) > 0 && (idKey.compareTo(preId) > 0 
						|| idKey.compareTo(pp.id) <= 0))))
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
			pp.messCount++;
			String messageId = pp.id.toString() + " " + Long.toString(pp.messCount);
			DChordForwardMessage fm = new DChordForwardMessage(om.taskId, messageId, 
					om.type, om.sender, om.key, om.value, 1);
			pp.hmUpStreams.put(fm.messageId, om.sender.getIndex());
			if (pp.rangeJudge(idKey, pp.id, ((PeerProtocol)pp.fingerTable[0].node.getProtocol(
					pp.par.pid)).id, true))
			{
				rangeJudSuccProc(fm, startTime);
			}
			else
			{
				rangeJudFailProc(fm, idKey, startTime);
			}
		}
	}
	
	public void operaFwdMsgProcess(DChordForwardMessage fm)
	{
		if (!pp.isUp())
		{
			operaNodeFail(fm);
			return;
		}
		pp.numRecFromOtherMsg++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		fm.numHops++;
		pp.hmUpStreams.put(fm.messageId, fm.sender.getIndex());
		BigInteger idKey = new BigInteger(fm.key);
		long startTime = CommonState.getTime();
		if (pp.rangeJudge(idKey, pp.id, ((PeerProtocol)pp.fingerTable[0].node.getProtocol(
				pp.par.pid)).id, true))
		{
			rangeJudSuccProc(fm, startTime);
		}
		else
		{
			rangeJudFailProc(fm, idKey, startTime);
		}
	}
	
	public void rangeJudSuccProc(LookUpSuccMessage lsm, long startTime)
	{
		pp.updateMaxFwdTime(Library.sendOverhead);
		ResLookUpMessage rlm = new ResLookUpMessage(lsm.messageId, 
				lsm.type, lsm.i, lsm.key, lsm.numHops, pp.fingerTable[0].node, true);
		if (!pp.fingerTable[0].alive)
		{
			rlm.success = false;
		}
		long endTime = pp.maxFwdTime;
		if (pp.updateGlobalQueue(node, startTime, endTime, rlm, lsm.sender))
		{
			pp.numFwdMsg++;
		}
	}
	
	public void rangeJudFailProc(LookUpSuccMessage lsm, long startTime)
	{
		pp.updateMaxFwdTime(Library.sendOverhead);
		Node nextNode = pp.closetPreNode(node, lsm.key);
		ResLookUpMessage rlm = null;
		LookUpSuccMessage lsmNew = null;
		if (nextNode == null)
		{
			rlm = new ResLookUpMessage(lsm.messageId, lsm.type, lsm.i, lsm.key, 
					lsm.numHops, null, false);
		}
		else
		{
			lsmNew = new LookUpSuccMessage(lsm.messageId, lsm.type, lsm.i, lsm.key, node, 
									lsm.numHops, nextNode.getIndex());
		}
		long endTime = pp.maxFwdTime;
		if (nextNode != null)
		{
			if (pp.updateGlobalQueue(node, startTime, endTime, lsmNew, nextNode))
			{
				pp.numFwdMsg++;
			}
		}
		else
		{
			if (pp.updateGlobalQueue(node, startTime, endTime, rlm, lsm.sender))
			{
				pp.numFwdMsg++;
			}
		}
	}
	
	public void lookUpSuccMsgProcess(LookUpSuccMessage lsm)
	{
		if (!pp.isUp())
		{
			ResLookUpMessage rlm = new ResLookUpMessage(lsm.messageId, 
					lsm.type, lsm.i, lsm.key, lsm.numHops, null, false);
			EDSimulator.add(Library.sendOverhead + 
					Library.commOverhead, rlm, lsm.sender, pp.par.pid);
			pp.numRecFromOtherMsgFailed++;
		}
		else 
		{
			lsm.numHops++;
			pp.hmUpStreams.put(lsm.messageId, lsm.sender.getIndex());
			pp.numRecFromOtherMsg++;
			pp.updateMaxFwdTime(Library.recvOverhead);
			BigInteger idKey = lsm.key;
			BigInteger preId = ((PeerProtocol)pp.predecessor.getProtocol(pp.par.pid)).id;
			long startTime = CommonState.getTime();
			pp.updateMaxFwdTime(Library.sendOverhead);
			long endTime = pp.maxFwdTime;
			if (((idKey.compareTo(pp.id) <= 0 && idKey.compareTo(preId) > 0) || 
					(preId.compareTo(pp.id) > 0 && (idKey.compareTo(preId) > 0 
							|| idKey.compareTo(pp.id) <= 0))))
			{
				ResLookUpMessage rlm = new ResLookUpMessage(lsm.messageId, 
						lsm.type, lsm.i, lsm.key, lsm.numHops, node, true);
				{
					if (pp.updateGlobalQueue(node, startTime, endTime, rlm, lsm.sender))
					{
						pp.numFwdMsg++;
					}
				}
			}
			else if (pp.rangeJudge(lsm.key, pp.id, ((PeerProtocol)pp.fingerTable[0].node.
														getProtocol(pp.par.pid)).id, true))
			{
				rangeJudSuccProc(lsm, startTime);
			}
			else
			{
				rangeJudFailProc(lsm, startTime);
			}
		}
	}
	
	public void resFwdMsgNodeFail(DChordResForwardMessage rfm, int upStreamId)
	{
		pp.numRecFromOtherMsgFailed++;
		if (upStreamId >= pp.numServer)
		{
			ResClientMessage rcm = new ResClientMessage(rfm.taskId, 
					rfm.type, rfm.key, rfm.value, false, true, rfm.numHops);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, rcm, 
					Network.get(upStreamId), pp.par.pid);
		}
		else
		{
			rfm.success = false;
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
												rfm, Network.get(upStreamId), pp.par.pid);
		}
		TaskDetail td = Library.taskHM.get(rfm.taskId);
		td.taskQueuedTime = CommonState.getTime();
		td.taskEndTime = CommonState.getTime();
	}
	
	public void resFwdMsgNodeOn(DChordResForwardMessage rfm, int upStreamId)
	{
		pp.updateMaxFwdTime(Library.recvOverhead);
		long startTime = CommonState.getTime();
		pp.updateMaxFwdTime(Library.sendOverhead);
		long endTime = pp.maxFwdTime;
		DChordOperaMessage com = null; 
		ResClientMessage rcm = null;
		if (upStreamId >= pp.numServer)
		{
			if (rfm.success)
			{
				com = new DChordOperaMessage(rfm.taskId, rfm.type, Network.get(upStreamId), 
									rfm.key, rfm.value, node.getIndex(), rfm.numHops + 1, false);
				if (pp.updateGlobalQueue(node, startTime, endTime, com, rfm.target))
				{
					pp.numFwdMsg++;
					pp.numFwdReq++;
				}
			}
			else
			{
				rcm = new ResClientMessage(rfm.taskId, rfm.type, rfm.key, rfm.value, false, true, rfm.numHops);
				if (pp.updateGlobalQueue(node, startTime, endTime, rcm, Network.get(upStreamId)))
				{
					pp.numFwdMsg++;
				}
			}
		}
		else
		{
			if (pp.updateGlobalQueue(node, startTime, endTime, rfm, Network.get(upStreamId)))
			{
				pp.numFwdMsg++;
			}
		}
	}
	
	public void resFwdMsgProcess(DChordResForwardMessage rfm)
	{
		int upStreamId = pp.hmUpStreams.get(rfm.messageId);
		if (!pp.isUp())
		{
			resFwdMsgNodeFail(rfm, upStreamId);
			return;
		}
		else
		{
			resFwdMsgNodeOn(rfm, upStreamId);
		}
	}
	
	public void resLookUpDownEventProc(int upStreamId, ResLookUpMessage rlm, 
			BigInteger bi, boolean exist, BigInteger sendId)
	{
		pp.numRecFromOtherMsgFailed++;
		if (pp.id.equals(sendId))
		{
			if (!rlm.type)
			{
				if (rlm.success && rlm.target != null)
				{
					UFTMessage uftm = new UFTMessage(rlm.type, rlm.i, 
							node.getIndex(), pp.succList[0].getIndex());
					BigInteger diff = pp.id.subtract(new BigInteger(
							Integer.toString(2)).pow(rlm.i - 1));
					if (bi.compareTo(diff) != 0)
					{
						exist = false;
					}
					Node dist = rlm.target;
					if (!exist)
					{
						dist = ((PeerProtocol)dist.getProtocol(pp.par.pid)).predecessor;
					}
					EDSimulator.add(Library.sendOverhead + 
							Library.commOverhead, uftm, dist, pp.par.pid);
				}
			}
		}
		else
		{
			rlm.success = false;
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
					rlm, Network.get(upStreamId), pp.par.pid);
			return;
		}
	}
	
	public void resLookUpMsgProcess(ResLookUpMessage rlm)
	{
		boolean exist = true;
		BigInteger bi = BigInteger.ZERO;
		String[] msg = rlm.messageId.split(" ");
		BigInteger sendId = new BigInteger(msg[0]);
		if (rlm.target != null)
		{
			bi = ((PeerProtocol)rlm.target.getProtocol(pp.par.pid)).id;
		}
		int upStreamId = -1;
		if (pp.hmUpStreams.containsKey(rlm.messageId))
		{
			upStreamId = pp.hmUpStreams.get(rlm.messageId);
		}
		if (!pp.isUp())
		{
			resLookUpDownEventProc(upStreamId, rlm, bi, exist, sendId);
			return;
		}
		pp.updateMaxFwdTime(Library.recvOverhead);
		long startTime = CommonState.getTime();
		pp.updateMaxFwdTime(Library.sendOverhead);
		long endTime = pp.maxFwdTime;
		UFTMessage uftm = null;
		BigInteger diff = pp.id.subtract(new BigInteger(Integer.toString(2)).pow(rlm.i - 1));
		if (bi.compareTo(diff) != 0)
		{
			exist = false;
		}
		Node dist = rlm.target;
		if (dist == null)
		{
			return;
		}
		if (!exist)
		{
			dist = ((PeerProtocol)dist.getProtocol(pp.par.pid)).predecessor;
		}
		if (pp.id.equals(sendId))
		{
			uftm = new UFTMessage(rlm.type, rlm.i, node.getIndex(), pp.succList[0].getIndex());
		}
		if (pp.id.equals(sendId))
		{
			if (pp.updateGlobalQueue(node, startTime, endTime, uftm, dist))
			{
				pp.numFwdMsg++;
			}
		}
		else
		{
			if (pp.updateGlobalQueue(node, startTime, endTime, rlm, Network.get(upStreamId)))
			{
				pp.numFwdMsg++;
			}
		}
	}
	
	public void chordOperaMsgProcess(DChordOperaMessage com)
	{
		String value = null;
		DChordResMessage rm = null;
		if (!pp.isUp())
		{
			pp.numRecReqFromOtherFailed++;
			pp.numRecFromOtherMsgFailed++;
			pp.numOtherReqFailed++;
			rm = new DChordResMessage(com.taskId, com.type, com.sender, com.key,
									value, false, true, com.numHops, node.getIndex(), -1);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
										rm, Network.get(com.servId), pp.par.pid);
			TaskDetail td = Library.taskHM.get(com.taskId);
			td.taskQueuedTime = CommonState.getTime();
			td.taskEndTime = CommonState.getTime();
			return;
		}
		pp.numRecReqFromOther++;
		pp.numRecFromOtherMsg++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		long startTime = CommonState.getTime();
		rm = (DChordResMessage)pp.actOperaMsgProcess(com, node);
		pp.procToFwd();
		pp.updateMaxFwdTime(Library.sendOverhead);
		long endTime = pp.maxFwdTime;
		if (pp.updateGlobalQueue(node, startTime, endTime, rm, Network.get(com.servId)))
		{
			pp.numOtherReqFinished++;
			pp.numFwdMsg++;
		}
	}
	
	public void chordResMsgProcess(DChordResMessage rm)
	{
		ResClientMessage rcm = new ResClientMessage(rm.taskId, rm.type, 
				rm.key, rm.value, rm.success, false, rm.numHops);
		long startTime = CommonState.getTime();
		if (!pp.isUp())
		{
			rcm.success = false;
			pp.numRecFromOtherMsgFailed++;
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
									rcm, rm.sender, pp.par.pid);
			return;
		}
		pp.numRecFromOtherMsg++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		pp.updateMaxFwdTime(Library.sendOverhead);
		long endTime = pp.maxFwdTime;
		if (pp.updateGlobalQueue(node, startTime, endTime, rcm, rm.sender))
		{
			pp.numFwdMsg++;
		}
	}
	
	public void uftMsgProcess(UFTMessage uftm)
	{
		if (!pp.isUp())
		{
			pp.numRecFromOtherMsgFailed++;
		}
		else
		{
			pp.numRecFromOtherMsg++;
			pp.updateMaxFwdTime(Library.recvOverhead);
			long startTime = CommonState.getTime();
			int id = pp.fingerTable[uftm.i - 1].node.getIndex();
			if (id == uftm.curId)
			{
				pp.fingerTable[uftm.i - 1].alive = uftm.type;
			}
			if (id >= uftm.curId)
			{
				if (pp.predecessor.isUp())
				{
					pp.updateMaxFwdTime(Library.sendOverhead);
					long endTime = pp.maxFwdTime;
					if (pp.updateGlobalQueue(node, startTime, endTime, uftm, pp.predecessor))
					{
						pp.numFwdMsg++;
					}
				}
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
			pp.churnNotMsgProcess(node);
			return;
		}
		if (event.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)event;
			operaMsgProcess(om);
		}
		else if (event.getClass() == DChordForwardMessage.class)
		{
			DChordForwardMessage fm = (DChordForwardMessage)event;
			operaFwdMsgProcess(fm);
		}
		else if (event.getClass() == LookUpSuccMessage.class)
		{
			LookUpSuccMessage lsm = (LookUpSuccMessage)event;
			lookUpSuccMsgProcess(lsm);
		}
		else if (event.getClass() == DChordResForwardMessage.class)
		{
			DChordResForwardMessage rfm = (DChordResForwardMessage)event;
			resFwdMsgProcess(rfm);
		}
		else if (event.getClass() == ResLookUpMessage.class)
		{
			ResLookUpMessage rlm = (ResLookUpMessage)event;
			resLookUpMsgProcess(rlm);
		}
		else if (event.getClass() == DChordOperaMessage.class)
		{
			DChordOperaMessage com = (DChordOperaMessage)event;
			chordOperaMsgProcess(com);
		}
		else if (event.getClass() == DChordResMessage.class)
		{
			DChordResMessage rm = (DChordResMessage)event;
			chordResMsgProcess(rm);
		}
		else if (event.getClass() == UFTMessage.class)
		{
			UFTMessage uftm = (UFTMessage)event;
			uftMsgProcess(uftm);
		}
		else if (event.getClass() == ResClientMessage.class)
		{
			ResClientMessage rcm = (ResClientMessage)event;
			pp.resClientMsgProcess(node, Library.file + "/summary_client_0_" + 
					Integer.toString(Network.size() - Library.offset) +  "_" + 
					pp.numReplica + "_" + Library.churnInterval + ".txt", rcm);
		}
	}
}