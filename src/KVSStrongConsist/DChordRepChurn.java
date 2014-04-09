import java.io.IOException;
import java.math.*;
import java.util.Iterator;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class DChordRepChurn 
{
	public Node node;
	public PeerProtocol pp;
	
	public DChordRepChurn(Node node, PeerProtocol pp)
	{
		this.node = node;
		this.pp = pp;
	}
	
	public void operaNodeFail(Object failEvent)
	{
		pp.numRecFromOtherMsgFailed++;
		ResClientMessageChurn rcmc = null;
		DChordResForwardMessage rfm = null;
		if (failEvent.getClass() == OperaMessage.class)
		{
			OperaMessage om = (OperaMessage)failEvent;
			int firstReplicaId = pp.replicaIds[0];
			rcmc = new ResClientMessageChurn(om.taskId, om.type, 
					om.key, om.value, false, true, 0, node.getIndex(), firstReplicaId);
			EDSimulator.add(Library.sendOverhead + 
					Library.commOverhead, rcmc, om.sender, pp.par.pid);
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
		DChordOperaMessage com = new DChordOperaMessage(fm.taskId, fm.type, fm.sender, fm.key, 
												fm.value, node.getIndex(), fm.numHops + 1, false);
		pp.updateMaxFwdTime(Library.sendOverhead);
		long endTime = pp.maxFwdTime;
		if (pp.updateGlobalQueue(node, startTime, endTime, com, pp.fingerTable[0].node))
		{
			pp.numFwdMsg++;
			pp.numFwdReq++;
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
		ResClientMessageChurn rcmc = null;
		if (nextNode == null)
		{
			if (fm.numHops > 1)
			{
				rfm = new DChordResForwardMessage(fm.taskId, fm.messageId, fm.type, 
					fm.key, fm.value, fm.numHops, null, false);
			}
			else
			{
				int replicaId = pp.replicaIds[0];
				rcmc = new ResClientMessageChurn(fm.taskId, fm.type, fm.key, fm.value, false,
						true, 0, node.getIndex(), replicaId);
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
				if (pp.updateGlobalQueue(node, startTime, endTime, rcmc, fm.sender))
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
		BigInteger idKey = new BigInteger(om.key);
		PeerProtocol ppTemp = (PeerProtocol)(Network.get(om.originId).getProtocol(pp.par.pid));
		BigInteger idResp = ppTemp.id;
		BigInteger preId = ((PeerProtocol)(ppTemp.predecessor.getProtocol(pp.par.pid))).id;
		long startTime = CommonState.getTime();
		if (pp.numServer == 1 || ((idKey.compareTo(idResp) <= 0 && idKey.compareTo(preId) > 0) || 
						(preId.compareTo(idResp) > 0 && (idKey.compareTo(preId) > 0 
						|| idKey.compareTo(idResp) <= 0))))
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
			int replicaId = pp.replicaIds[0];
			ResClientMessageChurn rcmc = new ResClientMessageChurn(rfm.taskId, rfm.type, rfm.key,
					rfm.value, false, true, rfm.numHops, node.getIndex(), replicaId);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, rcmc, 
					Network.get(upStreamId), pp.par.pid);
		}
		else
		{
			rfm.success = false;
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
												rfm, Network.get(upStreamId), pp.par.pid);
		}
	}
	
	public void resFwdMsgNodeOn(DChordResForwardMessage rfm, int upStreamId)
	{
		pp.updateMaxFwdTime(Library.recvOverhead);
		long startTime = CommonState.getTime();
		pp.updateMaxFwdTime(Library.sendOverhead);
		long endTime = pp.maxFwdTime;
		DChordOperaMessage com = null; 
		ResClientMessageChurn rcmc = null;
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
				rcmc = new ResClientMessageChurn(rfm.taskId, rfm.type, rfm.key, 
						rfm.value, false, true, rfm.numHops, node.getIndex(), pp.replicaIds[0]);
				if (pp.updateGlobalQueue(node, startTime, endTime, rcmc, Network.get(upStreamId)))
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
					UFTMessage uftm = new UFTMessage(rlm.type, 
							rlm.i, node.getIndex(), pp.succList[0].getIndex());
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
			rm = new DChordResMessage(com.taskId, com.type, com.sender, com.key, value, false, 
						true, com.numHops, node.getIndex(), pp.replicaIds[0]);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
										rm, Network.get(com.servId), pp.par.pid);
			TaskDetail td = Library.taskHM.get(com.taskId);
			td.taskQueuedTime = CommonState.getTime();
			td.taskEndTime = CommonState.getTime();
			return;
		}
		pp.numRecReqFromOther++;
		pp.numRecFromOtherMsg++;
		if (!com.queued)
		{
			pp.updateMaxFwdTime(Library.recvOverhead);
		}
		if (!pp.ready)
		{
			com.queued = true;
			Library.ll[node.getIndex()].add(com);
			return;
		}
		long startTime = CommonState.getTime();
		if (pp.numReplica == 0 || com.type == 0)
		{
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
		else
		{
			pp.actOperaMsgProcess(com, node);
		}
	}
	
	public void chordResMsgProcess(DChordResMessage rm)
	{
		long startTime = CommonState.getTime();
		if (!pp.isUp())
		{
			ResClientMessageChurn rcmc = new ResClientMessageChurn(rm.taskId, rm.type, rm.key, 
				rm.value, rm.success, false, rm.numHops, node.getIndex(), 
				pp.replicaIds[0]);
			pp.numRecFromOtherMsgFailed++;
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
									rcmc, rm.sender, pp.par.pid);
			return;
		}
		pp.numRecFromOtherMsg++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		pp.updateMaxFwdTime(Library.sendOverhead);
		long endTime = pp.maxFwdTime;
		if (!rm.success)
		{
			DChordOperaMessage dom = new DChordOperaMessage(rm.taskId, rm.type, rm.sender, 
					rm.key, rm.value, node.getIndex(), rm.numHops, false);
			int target = rm.curId;
			if (pp.hmTry.containsKey(rm.taskId))
			{
				Integer numTry = pp.hmTry.get(rm.taskId);
				numTry++;
				pp.hmTry.put(rm.taskId, numTry);
				if (numTry == pp.maxNumTry)
				{
					target = rm.firstReplicaId;
					pp.hmTry.remove(rm.taskId);
					if (target == node.getIndex())
					{
						chordOperaMsgProcess(dom);
						return;
					}
				}
			}
			else
			{
				pp.hmTry.put(rm.taskId, 1);
			}
			if (pp.updateGlobalQueue(node, startTime, endTime, dom, Network.get(target)))
			{
				pp.numFwdMsg++;
			}
		}
		else
		{
			ResClientMessage rcm = new ResClientMessage(rm.taskId, rm.type, 
					rm.key, rm.value, rm.success, false, rm.numHops);
			if (pp.updateGlobalQueue(node, startTime, endTime, rcm, rm.sender))
			{
				pp.numFwdMsg++;
			}
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
			if (uftm.type)
			{
				pp.fingerTable[uftm.i - 1].node = Network.get(uftm.curId);
				pp.fingerTable[uftm.i - 1].alive = true;
			}
			else 
			{
				if (id == uftm.curId)
				{
					pp.fingerTable[uftm.i - 1].node = Network.get(uftm.sucId); 
					pp.fingerTable[uftm.i - 1].alive = true;
				}
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
	
	public void repModMsgProcess(ReplicaModifyMessage rmm)
	{
		if (!pp.isUp())
		{
			return;
		}
		boolean has = false;
		int dist = 0;
		for (int i = 0;i < pp.numReplica;i++)
		{
			if (pp.replicaIds[i] == rmm.leaveId)
			{
				has = true;
				dist = pp.numReplica - i - 1;
				int[] temp = new int[pp.numReplica];
				for (int j = 0;j < pp.numReplica;j++)
				{
					temp[j] = pp.replicaIds[j];
				}
				for (int j = i;j < pp.numReplica - 1;j++)
				{
					pp.replicaIds[j] = temp[j + 1];
					pp.succList[j] = Network.get(temp[j + 1]);
				}
				pp.replicaIds[pp.numReplica - 1] = rmm.replicaId[dist];
				pp.sendData(node.getIndex(), node.getIndex(), dist, false);
			}
		}
		if (has)
		{
			long startTime = pp.maxFwdTime;
			pp.updateMaxFwdTime(Library.sendOverhead);
			long endTime = pp.maxFwdTime;
			if (pp.updateGlobalQueue(node, startTime, endTime, rmm, pp.predecessor))
			{
				pp.numFwdMsg++;
			}
		}
	}
	
	public void joinNotMsgProcess(JoinNotifyMessage jnm)
	{
		if (!pp.isUp())
		{
			return;
		}
		if (pp.hmAllData.containsKey(jnm.joinId))
		{
			pp.sendData(node.getIndex(), node.getIndex(), jnm.joinId, true);
			pp.updateMaxFwdTime(Library.sendOverhead);
			long startTime = CommonState.getTime();
			long endTime = pp.maxFwdTime;
			DataDeleteMessage ddm = new DataDeleteMessage(jnm.joinId);
			if (pp.updateGlobalQueue(node, startTime, endTime, ddm, 
					Network.get(pp.replicaIds[pp.numReplica - 1])))
			{
				pp.numFwdMsg++;
			}
		}
		else
		{
			boolean has = false;
			for (int i = 0;i < pp.numReplica;i++)
			{
				if (pp.replicaIds[i] > jnm.joinId)
				{
					int[] temp = new int[pp.numReplica];
					for (int j = 0;j < pp.numReplica;j++)
					{
						temp[j] = pp.replicaIds[j];
					}
					has = true;
					pp.replicaIds[i] = jnm.joinId;
					for (int j = i + 1;j < pp.numReplica;j++)
					{
						pp.replicaIds[j] = temp[j - 1];
						pp.succList[j] = Network.get(temp[j - 1]);
					}
					DataDeleteMessage ddm = new DataDeleteMessage(node.getIndex());
					long startTime = CommonState.getTime();
					pp.updateMaxFwdTime(Library.sendOverhead);
					long endTime = pp.maxFwdTime;
					if (pp.updateGlobalQueue(node, startTime, endTime, 
							ddm, Network.get(temp[pp.numReplica - 1])))
					{
						pp.numFwdMsg++;
					}
					break;
				}
			}
			if (has)
			{
				long startTime = pp.maxFwdTime;
				pp.updateMaxFwdTime(Library.sendOverhead);
				long endTime = pp.maxFwdTime;
				if (pp.updateGlobalQueue(node, startTime, endTime, jnm, pp.predecessor))
				{
					pp.numFwdMsg++;
				}
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
				pp.succList[0] = Network.get(dfm.senderId);
				for (int i = 1;i < pp.numReplica;i++)
				{
					pp.replicaIds[i] = dfm.myReplicaIds[i - 1];
					pp.succList[i] = Network.get(pp.replicaIds[i]);
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
				int distId = drfm.myReplicas[0];
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
		if (pp.isUp())
		{
			pp.updateMaxFwdTime(Library.recvOverhead);
		}
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
			else if (obj.getClass() == DChordOperaMessage.class)
			{
				DChordOperaMessage com = (DChordOperaMessage)obj;
				com.queued = true;
				if (!pp.ready)
				{
					Library.ll[node.getIndex()].add(com);
				}
				else
				{
					chordOperaMsgProcess(com);
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
			else if (obj.getClass() == DChordOperaMessage.class)
			{
				DChordOperaMessage com = (DChordOperaMessage)obj;
				chordOperaMsgProcess(com);
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
		else if (event.getClass() == DChordForwardMessage.class)
		{
			Library.numOperaMsg++;
			DChordForwardMessage fm = (DChordForwardMessage)event;
			operaFwdMsgProcess(fm);
		}
		else if (event.getClass() == LookUpSuccMessage.class)
		{
			Library.numChurnMsg++;
			LookUpSuccMessage lsm = (LookUpSuccMessage)event;
			lookUpSuccMsgProcess(lsm);
		}
		else if (event.getClass() == DChordResForwardMessage.class)
		{
			Library.numOperaMsg++;
			DChordResForwardMessage rfm = (DChordResForwardMessage)event;
			resFwdMsgProcess(rfm);
		}
		else if (event.getClass() == ResLookUpMessage.class)
		{
			Library.numChurnMsg++;
			ResLookUpMessage rlm = (ResLookUpMessage)event;
			resLookUpMsgProcess(rlm);
		}
		else if (event.getClass() == DChordOperaMessage.class)
		{
			Library.numOperaMsg++;
			DChordOperaMessage com = (DChordOperaMessage)event;
			chordOperaMsgProcess(com);
		}
		else if (event.getClass() == DChordResMessage.class)
		{
			Library.numOperaMsg++;
			DChordResMessage rm = (DChordResMessage)event;
			chordResMsgProcess(rm);
		}
		else if (event.getClass() == UFTMessage.class)
		{
			Library.numChurnMsg++;
			UFTMessage uftm = (UFTMessage)event;
			uftMsgProcess(uftm);
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
		else if (event.getClass() == LeaveNotifyMessage.class)
		{
			Library.numChurnMsg++;
			LeaveNotifyMessage lnm = (LeaveNotifyMessage)event;
			if (!pp.isUp())
			{
				return;
			}
			pp.predecessor = Network.get(lnm.preId);
			pp.sendData(node.getIndex(), lnm.leaveId, pp.replicaIds[pp.numReplica - 1], false);
		}
		else if (event.getClass() == ReplicaModifyMessage.class)
		{
			Library.numChurnMsg++;
			ReplicaModifyMessage rmm = (ReplicaModifyMessage)event;
			repModMsgProcess(rmm);
		}
		else if (event.getClass() == JoinNotifyMessage.class)
		{
			Library.numChurnMsg++;
			JoinNotifyMessage jnm = (JoinNotifyMessage)event;
			joinNotMsgProcess(jnm);
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
		else if (event.getClass() == TransReqMessage.class)
		{
			Library.numStrongConsistMsg++;
			TransReqMessage trm = (TransReqMessage)event;
			transReqMsgProcess(trm);
		}
	}
}