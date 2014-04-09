import java.io.IOException;
import java.math.*;
import java.util.Iterator;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class DFCChurn 
{
	public Node node;
	public PeerProtocol pp;
	
	public DFCChurn(Node node, PeerProtocol pp)
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
	
	public void operaMsgProcess(OperaMessage om)
	{
		pp.numLocalReq++;
		if (!pp.isUp())
		{
			pp.numLocalReqFailed++;
			pp.numRecFromOtherMsgFailed++;
			ResClientMessage orm = new ResClientMessage(om.taskId, 
					om.type, om.key, om.value, false, true, 0);
			TaskDetail td = Library.taskHM.get(om.taskId);
			td.taskQueuedTime = CommonState.getTime();
			td.taskEndTime = CommonState.getTime();
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
									orm, om.sender, pp.par.pid);
		}
		else
		{
			pp.numRecFromOtherMsg++;
			pp.updateMaxFwdTime(Library.recvOverhead);
			int target = pp.hashServer(new BigInteger(om.key), pp.numServer);
			long startTime = CommonState.getTime();
			if (target == node.getIndex())
			{
				ResClientMessage orm = (ResClientMessage)pp.actOperaMsgProcess(om, node);
				pp.procToFwd();
				pp.updateMaxFwdTime(Library.sendOverhead);
				long endTime = pp.maxFwdTime;
				if (pp.updateGlobalQueue(node, startTime, endTime, orm, om.sender))
				{
					pp.numLocalReqFinished++;
					pp.numFwdMsg++;
				}
			}
			else if (pp.memList.containsKey(new Integer(target)))
			{
				DFCForwardMessage fm = new DFCForwardMessage(om.taskId, 
						om.sender, node.getIndex(), om.type, om.key, om.value, 1, false);
				fwdProcessing(startTime, fm, target);
			}
			else
			{
				pp.updateMaxFwdTime(Library.sendOverhead);
				ResClientMessage orm = new ResClientMessage(om.taskId, om.type, om.key, 
														om.value, false, true, 0);
				long endTime = pp.maxFwdTime;
				TaskDetail td = Library.taskHM.get(om.taskId);
				td.taskQueuedTime = CommonState.getTime();
				if (pp.updateGlobalQueue(node, startTime, endTime, orm, om.sender))
				{
					pp.numLocalReqFailed++;
					pp.numFwdMsg++;
					td.taskEndTime = endTime - Library.sendOverhead;
				}
				return;
			}
		}
	}
	
	public void fwdMsgProcess(DFCForwardMessage fm)
	{
		DFCResForwardMessage rfm;
		if (!pp.isUp())
		{
			rfm = new DFCResForwardMessage(fm.taskId, fm.source, fm.type, fm.key, fm.value, 
											false, fm.numHops, node.getIndex());
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, rfm, 
										Network.get(fm.intermid), pp.par.pid);
			pp.numRecReqFromOtherFailed++;
			pp.numRecFromOtherMsgFailed++;
			TaskDetail td = Library.taskHM.get(fm.taskId);
			td.taskQueuedTime = CommonState.getTime();
			td.taskEndTime = CommonState.getTime();
			return;
		}
		pp.numRecReqFromOther++;
		pp.numRecFromOtherMsg++;
		pp.updateMaxFwdTime(Library.recvOverhead);
		long startTime = CommonState.getTime();
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
	
	public void resFwdMsgProcess(DFCResForwardMessage rfm)
	{
		ResClientMessage rcm = null;
		if (!pp.isUp())
		{
			pp.numRecFromOtherMsgFailed++;
			rcm = new ResClientMessage(rfm.taskId, rfm.type, rfm.key, 
											rfm.value, false, false, rfm.numHops);
			EDSimulator.add(Library.sendOverhead + Library.commOverhead, 
									rcm, rfm.source, pp.par.pid);
		}
		else
		{
			pp.numRecFromOtherMsg++;
			pp.updateMaxFwdTime(Library.recvOverhead);
			rcm = new ResClientMessage(rfm.taskId, rfm.type, rfm.key, 
											rfm.value, rfm.success, false, rfm.numHops);
			long startTime = CommonState.getTime();
			pp.updateMaxFwdTime(Library.sendOverhead);
			long endTime = pp.maxFwdTime;
			if (pp.updateGlobalQueue(node, startTime, endTime, rcm, rfm.source))
			{
				pp.numFwdMsg++;
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
			if (dist != node.getIndex())
			{
				startTime = pp.maxFwdTime + Library.sendOverhead * i;
				endTime = startTime + Library.sendOverhead;
				
				if (endTime <= Library.currChurnTime || startTime >= Library.currChurnTime 
						&& endTime <= Library.nextChurnTime)
				{
					EDSimulator.add(endTime - CommonState.getTime() + Library.commOverhead, 
							ncmForward, Network.get(dist), pp.par.pid);
					pp.numFwdMsg++;
				}
				else
				{
					Library.waitQueue.get(node.getIndex()).add(new WaitingEvent(
							dist, startTime, endTime, Library.commOverhead, ncmForward));
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
		pp.updateMaxTime(Library.procTime);
		if (ncm.notify)
		{
			pp.procToFwd();
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
		else if (event.getClass() == ResClientMessage.class)
		{
			ResClientMessage rcm = (ResClientMessage)event;
			pp.resClientMsgProcess(node, Library.file + "/summary_client_0_" + 
						Integer.toString(Network.size() - Library.offset) +  "_" + 
						pp.numReplica + "_" + Library.churnInterval + ".txt", rcm);
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
		else if (event.getClass() == NetChurnMessage.class)
		{
			NetChurnMessage ncm = (NetChurnMessage)event;
			netChurnMsgProcess(ncm);
		}
		else if (event.getClass() == MemMessage.class)
		{
			MemMessage mm = (MemMessage)event;
			memMsgProcess(mm);
		}
	}
}