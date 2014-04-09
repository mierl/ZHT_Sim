import java.io.*;
import java.math.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class Library 
{
	public static long netSpeed;
	public static long latency;
	public static long messSize;
	public static long commOverhead;
	public static long sendOverhead;
	public static long recvOverhead;
	public static long clientSendOverhead;
	public static long clientRecvOverhead;
	public static long allCommOverhead;
	public static long procTime;
	
	public static long logInterval;
	public static long churnInterval;
	public static long preNoOperaFinished;
	public static long numOperaFinished;
	public static long numOperaFailed;
	public static long numAllMessage;
	public static long numOperaMsg;
	public static long numChurnMsg;
	public static long numStrongConsistMsg;
	public static long numEvenConsistMsg;
	public static long numAllOpera;
	public static long numJoins;
	public static long numLeaves;
	
	public static long currChurnTime;
	public static long nextChurnTime;
	public static long numActiveServer;
	public static long taskId;
	
	public static boolean eager;
	public static int type;
	public static int offset;
	public static BigInteger upperBound;
	public static String file;
	public static TaskDescription[][] td;
	public static double[] proTimeArray;
	public static int numClientPerServ;
	
	public static BufferedWriter bwZeroTask;
	public static BufferedWriter bwSummary; 
	public static BufferedWriter bwServer;
	public static BufferedWriter bwClient;
	public static BufferedWriter bwTask;
	public static BufferedWriter bwSumStat;
	public static BufferedReader br;
	public static BufferedReader brProcTime;
	public static HashMap<Integer, Boolean> memList;
	public static HashMap<Integer, Boolean> leaveList; 
	public static HashMap<Integer, LinkedList<WaitingEvent>> waitQueue;
	public static HashMap<Integer, LinkedList<WaitingEvent>> waitQueueConsist;
	public static HashMap<Long, TaskDetail> taskHM;
	public static LinkedList<Object>[] ll;
	
	@SuppressWarnings("unchecked")
	public static void initLib(int numServer, int numOpera, int numReplica, int numClientPerServ)
	{
		Library.netSpeed = 6800000000L;
		Library.latency = 100L;
		Library.messSize = 10240L;
		Library.commOverhead = Library.messSize * 8L * 1000000L 
				/ Library.netSpeed + Library.latency;
		Library.clientRecvOverhead = Library.recvOverhead = 
				Library.sendOverhead = Library.clientSendOverhead = 50L;
		Library.allCommOverhead = Library.sendOverhead + Library.commOverhead + 
										Library.recvOverhead;
		
		Library.logInterval = 100000L;
		Library.preNoOperaFinished = 0;
		Library.numOperaFinished = 0;
		Library.numOperaFailed = 0;
		Library.numAllMessage = 0;
		Library.numOperaMsg = 0;
		Library.numChurnMsg = 0;
		Library.numStrongConsistMsg = 0;
		Library.numEvenConsistMsg = 0;
		Library.numJoins = 0;
		Library.numLeaves = 0;
		
		Library.offset = numServer;
		if (Library.type == 0 || Library.type == 1 || Library.type == 6 || Library.type == 7)
		{
			Library.offset += numReplica;
		}
		int numClient  = Network.size() - Library.offset;
		Library.numAllOpera = (long)numClient * (long)numOpera;
		td = new TaskDescription[numClient][numOpera];
		proTimeArray = new double[21];
		
		Library.currChurnTime = 5000L;
		Library.nextChurnTime = Library.currChurnTime + Library.churnInterval;
		Library.numActiveServer = numServer;
		Library.taskId = 0;
		Library.eager = true;
		
		switch (Library.type)
		{
			case 0:
				Library.file = "./csingle";
				Library.numClientPerServ = numClient;
				break;
			case 1:
				Library.file = "./ctree";
				Library.numClientPerServ = numClient;
				break;
			case 2: 
				Library.file = "./dfc";
				Library.numClientPerServ = numClient / numServer;
				break;
			case 3:
				Library.file = "./dchord";
				Library.numClientPerServ = numClient / numServer;
				break;
			case 4:
				Library.file = "./dfcchurn";
				Library.numClientPerServ = numClient / numServer;
				break;
			case 5:
				Library.file = "./dchordchurn";
				Library.numClientPerServ = numClient / numServer;
				break;
			case 6:
				Library.file = "./csinglerepchurn";
				Library.numClientPerServ = numClient / numServer;
				break;
			case 7:
				Library.file = "./ctreerepchurn";
				Library.numClientPerServ = numClient / numServer;
				break;
			case 8:
				Library.file = "./dfcrepchurnslurm";
				Library.numClientPerServ = numClient / numServer;
				break;
			case 9:
				Library.file = "./dchordrepchurnslurm";
				Library.numClientPerServ = numClient / numServer;
				break;
		}
		
		try
		{
			Library.bwZeroTask = new BufferedWriter(new FileWriter(file + "/summary_client_0_" + 
										Integer.toString(numClient) + "_" + numReplica + "_" + 
										Library.churnInterval + ".txt"));
			Library.bwZeroTask.write("Task#" + "\t" + "Start/End Time (us)" + "\r\n");
			Library.bwSummary = new BufferedWriter(new FileWriter(file + "/summary_" + 
										Integer.toString(numClient) + "_" + numReplica + "_" + 
										Library.churnInterval + ".txt"));
			Library.bwSummary.write("SimuTime" + "\t" + "NumAllServ" + "\t" + "NumActServ" + 
									"\t" + "NumTaskFin" + "\t" + "Thr" + "\t" + "NumMsg" + 
									"\t" + "NumFailedTask" + "\t" + "NumJoins" + "\t" + 
									"NumLeaves" + "\r\n");
			Library.bwServer = new BufferedWriter(new FileWriter(file + "/servers_statistics_" + 
										Integer.toString(numClient) + "_" + numReplica + "_" + 
										Library.churnInterval + ".txt"));
			Library.bwServer.write("ServerId" + "\t" + "NumLocalReq" + "\t" + "NumLocalReqFin" + 
									"\t" + "NumLocalReqFailed" + "\t" + "NumFwdMsg" + "\t" + 
									"NumFwdMsgFailed" + "\t" + "NumFwdReq" + "\t" + 
									"NumFwdReqFailed" + "\t" + "NumRecFromOtherMsg" + "\t" +
									"NumRecFromOtherMsgFailed" + "\t" + "NumRecReqFromOther" + 
									"\t" + "NumRecReqFromOtherFailed" + "\t" + 
									"NumOtherReqFinished" + "\t" + "NumOtherReqFailed" + "\t" + 
									"NumAllReqFinished" + "\t" + "NumAllReqFailed" + "\t" + 
									"Thr" + "\r\n");
			Library.bwClient = new BufferedWriter(new FileWriter(file + "/client_statistics_" +
										Integer.toString(numClient) + "_" + numReplica + "_" + 
										Library.churnInterval + ".txt"));
			Library.bwClient.write("ClientId" + "\t" + "NumOperaFinished" + "\t" + 
										"NumOperaFailed" + "\t" + "Throughput" + "\r\n");
			Library.bwTask = new BufferedWriter(new FileWriter(file + "/task_" + 
										Integer.toString(numClient) + "_" + numReplica + "_" + 
										Library.churnInterval + ".txt"));
			Library.bwTask.write("TaskId" + "\t" + "ClientId" + "\t" + "TaskSuccess" + "\t" + 
									"TaskType" + "\t" + "TaskSubmitTime" + "\t" + 
									"TaskQueuedTime" + "\t" + "TaskEndTime" + "\t" + 
									"TaskBackClientTime" + "\t" + "NunHops" + "\r\n");
			Library.bwSumStat = new BufferedWriter(new FileWriter(file + "/summary_statistics_" +
										Integer.toString(numClient) + "_" + numReplica + "_" + 
										Library.churnInterval + ".txt"));
			Library.bwSumStat.write("MinTAT" + "\t" + "AveTAT" + "\t" + "MaxTAT" + "\t" + 
								     "SimuTime" + "\t" + "ClientThr" + "\t" + "MinServThr" + 
								     "\t" + "AveServThr" + "\t" + "MaxServThr" + "\t" + 
								     "MinLoad" + "\t" + "AveLoad" + "\t" + "MaxLoad" + "\t" + 
								     "MinFailRatio" + "\t" + "AveFailRatio" + "\t" + 
								     "MaxFailRatio" + "\t" + "NumJoins" + "\t" + 
								     "NumLeaves" + "\t" + "NumMsg" + "\t" + "NumOperaMsg" + "\t" + 
								     "NumChurnMsg" + "\t" + "NumStrongConsistMsg" + "\t" + 
								     "NumEvenConsistMsg" + "\r\n");
			Library.br = new BufferedReader(new FileReader("/home/kwang/Documents/simulation" +
											"/workspace/Test/task.txt"));
			Library.brProcTime = new BufferedReader(new FileReader("/home/kwang/Documents/" +
											"simulation/workspace/Test/InputFile.txt"));
			//Library.br = new BufferedReader(new FileReader("./startup/task.txt"));
			//Library.brProcTime = new BufferedReader(new FileReader("./startup/InputFile.txt"));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		Library.memList = new HashMap<Integer, Boolean>();
		for (int i = 0;i < numServer;i++)
		{
			Library.memList.put(i, null);
		}
		Library.leaveList = new HashMap<Integer, Boolean>();
		Library.waitQueue = new HashMap<Integer, LinkedList<WaitingEvent>>(offset);
		Library.waitQueueConsist = new HashMap<Integer, LinkedList<WaitingEvent>>(offset);
		Library.ll = new LinkedList[offset];
		for (int i = 0;i < offset;i++)
		{
			Library.waitQueue.put(i, new LinkedList<WaitingEvent>());
		}
		for (int i = 0;i < offset;i++)
		{
			Library.waitQueueConsist.put(i, new LinkedList<WaitingEvent>());
			Library.ll[i] = new LinkedList<Object>();
		}
		Library.taskHM = new HashMap<Long, TaskDetail>();
		try
		{
			for (int i = 0;i < numClient;i++)
			{
				for (int j = 0;j < numOpera;j++)
				{
					String[] str = br.readLine().split("\t");
					Library.td[i][j] = new TaskDescription(str[0], str[1], Integer.parseInt(str[2]));
				}
			}
			for (int i = 0;i < 11;i++)
			{
				Library.proTimeArray[i] = Double.parseDouble(brProcTime.readLine());
			}
			for (int i = 11;i < 21;i++)
			{
				Library.proTimeArray[i] = Library.proTimeArray[i - 1] / (1.584 / 2); 
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		int posIdx = (int)(Math.log(Library.numClientPerServ) / Math.log(2));
		Library.procTime = (long)(Library.proTimeArray[posIdx] + 0.5);
	}
	
	public static  long getPackCommOverhead(int numMsg)
	{
		return Library.messSize * 8L * 1000000L * (long)numMsg 
		/ Library.netSpeed + Library.latency;
	}
	
	public static void logTask(int id, boolean type, int pid, long wait)
	{
		Node node = Network.get(id);
		PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
		long taskId = pp.numLocalReqFinished + pp.numLocalReqFailed;
		long time = CommonState.getTime() + wait;
		if (type)
		{
			taskId++;
		}
		try
		{
			Library.bwZeroTask.write(taskId + "\t" + Long.toString(time) + "\r\n");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/* calculate the client 0 task turn-around time */
	public static TurnaroundTime turnaroundTime (String path, int numOpera) throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader(path));
		double sum = 0;
		double max = -10000000000.0;
		double min = 10000000000.0;
		String str = br.readLine();
		str = br.readLine();
		while (str != null)
		{
			String[] strLine = str.split("\t");
			long start = Long.parseLong(strLine[1]);
			str = br.readLine();
			strLine = str.split("\t");
			long end = Long.parseLong(strLine[1]);
			double time = end - start;
			sum += time;
			if (time > max)
			{
				max = time;
			}
			if (time < min)
			{
				min = time;
			}
			str = br.readLine();
		}
		double average = (double)sum / numOpera;
		return new TurnaroundTime(min, max, average);
	}
	
	/* print the client 0 task turn-around time to file */
	public static void printTAT(TurnaroundTime tt)
	{
		try
		{
			Library.bwSumStat.write(Double.toString(tt.minTATime) + "\t" + 
				Double.toString(tt.aveTATime) + "\t" + Double.toString(tt.maxTATime) + "\t");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/* summary log */
	public static void log(long time, int numServer)
	{
		double throughput = (double)(Library.numOperaFinished - Library.preNoOperaFinished) 
				/ (double)Library.logInterval * 1E6;							
		Library.preNoOperaFinished = Library.numOperaFinished;
		try
		{
			Library.bwSummary.write(time + "\t" + numServer + "\t" + Library.numActiveServer + "\t"
						+ Library.numOperaFinished + "\t" + throughput + "\t" 
						+ Library.numAllMessage + "\t" + Library.numOperaFailed + "\t"
						+ Library.numJoins + "\t" + Library.numLeaves + "\r\n");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/* log the task specification */
	public static void logTask(TaskDetail td)
	{
		try
		{
			Library.bwTask.write(td.taskId + "\t" + (td.clientId - Library.offset) + "\t" 
								+ td.taskSuccess + "\t" + td.taskType + "\t" 
								+ td.taskSubmitTime + "\t" + td.taskQueuedTime 
								+ "\t" + td.taskEndTime + "\t" + td.taskBackClientTime 
								+ "\t" + td.numHops + "\r\n");
			Library.bwTask.flush();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/* calculate the throughput of each server */
	public static void calServThroughput(int pid)
	{
		for (int i = 0;i < Library.offset;i++)
		{
			Node node = Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			pp.numAllReqFinished = pp.numLocalReqFinished + pp.numOtherReqFinished;
			pp.throughput = (double)pp.numAllReqFinished / (double)
					(CommonState.getTime() + Library.clientRecvOverhead) * 1E6;
		}
	}
	
	/* log statistics of each server */
	public static void logServStatistics(int pid) throws IOException
	{
		for (int i = 0;i < Library.offset;i++)
		{
			Node node = Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			Library.bwServer.write(pp.id + "\t" + pp.numLocalReq + 
						"\t" + pp.numLocalReqFinished + "\t" + pp.numLocalReqFailed + 
						"\t" + pp.numFwdMsg + "\t" + pp.numFwdMsgFailed + 
						"\t" + pp.numFwdReq + "\t" + pp.numFwdReqFailed + 
						"\t" + pp.numRecFromOtherMsg + "\t" + pp.numRecFromOtherMsgFailed + 
						"\t" + pp.numRecReqFromOther + "\t" + pp.numRecReqFromOtherFailed + 
						"\t" + pp.numOtherReqFinished + "\t" + pp.numOtherReqFailed + 
						"\t" + pp.numAllReqFinished + "\t" + pp.numAllReqFailed + 
						"\t" + pp.throughput + "\r\n");
		}
		Library.bwServer.flush();
		Library.bwServer.close();
	}
	
	/* print the server throughput to a file */
	public static void printServStatistics(int pid)
	{
		double minThr = 100000000000.0;
		double maxThr = -100000000000.0;
		double sumThr = 0.0;
		for (int i = 0;i < Library.offset;i++)
		{
			Node node = Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			if (pp.throughput > maxThr)
			{
				maxThr = pp.throughput;
			}
			if (pp.throughput < minThr)
			{
				minThr = pp.throughput;
			}
			sumThr += pp.throughput;
		}
		try
		{
			Library.bwSumStat.write(Double.toString(minThr) + "\t" + Double.toString(
					sumThr / Library.offset) + "\t" + Double.toString(maxThr) + "\t");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/* log the statistics of each client */
	public static void logClientStatistics(int numClient, int pid) throws IOException
	{
		for (int i = 0;i < numClient;i++)
		{
			Node node = Network.get(i + Library.offset);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			Library.bwClient.write(i + "\t" + pp.numLocalReqFinished + "\t" + 
									pp.numLocalReqFailed + "\t" + pp.clientThroughput + "\r\n");
		}
		Library.bwClient.flush();
		Library.bwClient.close();
	}
	
	/* print the load of each server */
	public static void printLoad(int pid)
	{
		long maxLoad = -1000000000;
		long minLoad = 1000000000;
		long sumLoad = 0;
		
		for (int i = 0;i < Library.offset;i++)
		{
			Node node = Network.get(i); 
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			if (pp.numAllReqFinished > maxLoad)
			{
				maxLoad = pp.numAllReqFinished;
			}
			if (pp.numAllReqFinished < minLoad)
			{
				minLoad = pp.numAllReqFinished;
			}
			sumLoad += pp.numAllReqFinished;
		}
		try
		{
			Library.bwSumStat.append(Long.toString(minLoad) + "\t" + Double.toString(
					(double)sumLoad / (double)Library.offset) + "\t" + Double.toString(
					maxLoad) + "\t");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/* print the failure rate of each client */
	public static void printFailRate(int numServer, int pid, int numOpera)
	{
		double minFailRate = 10000000000.0;
		double maxFailRate = -10000000000.0;
		double sumFailRate = 0.0;

		for (int i = numServer;i < Network.size();i++)
		{
			Node node = Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			double failRate = (double)pp.numLocalReqFailed / (double)numOpera;
			if (failRate < minFailRate)
			{
				minFailRate = failRate;
			}
			if (failRate > maxFailRate)
			{
				maxFailRate = failRate;
			}
			sumFailRate += failRate;
		}
		try
		{
			Library.bwSumStat.append(Double.toString(minFailRate) + "\t" + 
					Double.toString(sumFailRate / (Network.size() - 
					numServer)) + "\t" + Double.toString(maxFailRate) + "\t");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/* print the client perceived throughput to a file */
	public static void printClientThr(long simuTime)
	{
		try
		{
			Library.bwSumStat.write(Double.toString(
				(double)Library.numOperaFinished / simuTime * 1E6) + "\t");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/* print the number of churn events */
	public static void printChurn()
	{
		try
		{
			Library.bwSumStat.write(Long.toString(Library.numJoins) + "\t" + 
				Long.toString(Library.numLeaves) + "\t");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	/* choose the server to do churn */
	public static Node selChurnNode(int numServer)
	{
		int nodeIndex = CommonState.r.nextInt(numServer);
		if (Library.type == 6 || Library.type == 7)
		{
			nodeIndex = 0;
		}
		return Network.get(nodeIndex);
	}
	
	public static void processEvent(Node node, int pid, Object event)
	{
		PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
		pp.par.pid = pid;
		switch (Library.type)
		{
			case 0: 
				new CSingle(node, pp).processEvent(event);
				break;
			case 1:
				new CTree(node, pp).processEvent(event);
				break;
			case 2:
				new DFC(node, pp).processEvent(event);
				break;
			case 3:
				new DChord(node, pp).processEvent(event);
				break;
			case 4:
				new DFCChurn(node, pp).processEvent(event);
				break;
			case 5:
				new DChordChurn(node, pp).processEvent(event);
				break;
			case 6:
				new CSingleRepChurn(node, pp).processEvent(event);
				break;
			case 7:
				new CTreeRepChurn(node, pp).processEvent(event);
				break;
			case 8:
				new DFCRepChurn(node, pp).processEvent(event);
				break;
			case 9:
				new DChordRepChurn(node, pp).processEvent(event);
				break;
		}
	}
	
	public static void csinglechurn(boolean type, Node newNode, int pid)
	{
		PeerProtocol pp = (PeerProtocol)newNode.getProtocol(pid);
		Integer intNum = new Integer(newNode.getIndex());
		if (type)
		{
			pp.setUp();
			pp.memList.put(intNum, null);
			Library.memList.put(intNum, null);
			Library.leaveList.remove(intNum);
			pp.updateMaxFwdTime(Library.sendOverhead);
			ReloadDataMessage rdm = new ReloadDataMessage(newNode.getIndex());
			long startTime = CommonState.getTime();
			long endTime = pp.maxFwdTime;
			if (pp.updateGlobalQueue(newNode, startTime, endTime, rdm, Network.get(intNum + 1)))
			{
				pp.numFwdMsg++;
			}
		}
		else
		{
			pp.setDown();
			pp.memList.remove(intNum);
			Library.leaveList.put(intNum, null);
			Library.memList.remove(intNum);
			pp.hmData.clear();
		}
	}
	
	public static void dfcChurn(boolean type, Node newNode, int pid)
	{
		PeerProtocol pp = (PeerProtocol)newNode.getProtocol(pid);
		if (type)
		{
			pp.setUp();
			pp.memList.put(newNode.getIndex(), null);
			Library.memList.put(newNode.getIndex(), null);
			Library.leaveList.remove(newNode.getIndex());
		}
		else
		{
			pp.setDown();
			pp.memList.remove(newNode.getIndex());
			Library.leaveList.put(newNode.getIndex(), null);
			Library.memList.remove(newNode.getIndex());
		}
		if (Library.eager)
		{
			int size = Library.memList.size();
			Set<Integer> set = Library.memList.keySet();
			Integer[] intArray = new Integer[size];
			set.toArray(intArray);
			if (size > 0)
			{
				int selected = CommonState.r.nextInt(size);
				if (size == 1 && intArray[selected] == newNode.getIndex())
				{
					return;
				}
				PeerProtocol pp1 = (PeerProtocol)(Network.
						get(intArray[selected]).getProtocol(pid));
				while (intArray[selected] == newNode.getIndex() || !pp1.isUp())
				{
					selected = CommonState.r.nextInt(size);
					pp1 = (PeerProtocol)(Network.get(intArray[selected]).getProtocol(pid));
				}
				Node knownServ = Network.get(intArray[selected]);
				NetChurnMessage ncm = new NetChurnMessage(type, newNode.getIndex(), true);
				EDSimulator.add(Library.sendOverhead + Library.commOverhead, ncm, knownServ, pid);
			}
		}
	}
	
	public static void dchordChurn(boolean type, Node newNode, int idLength, int pid)
	{
		PeerProtocol pp = (PeerProtocol)newNode.getProtocol(pid);
		if (type)
		{
			pp.setUp();
			Library.memList.put(newNode.getIndex(), null);
			Library.leaveList.remove(newNode.getIndex());
		}
		else
		{
			pp.setDown();
			Library.leaveList.put(newNode.getIndex(), null);
			Library.memList.remove(newNode.getIndex());
		}
		if (Library.eager)
		{
			if (Library.memList.size() == 0 || (Library.memList.size()
				== 1 && Network.size() == 1))
			{
				return;
			}
			BigInteger bi = null;
			BigInteger two = new BigInteger(Integer.toString(2));
			for (int i = 1;i <= idLength;i++)
			{
				BigInteger temp = BigInteger.ONE;
				for (int j = 0;j < i - 1;j++)
				{
					temp = temp.multiply(two);
				}
				bi = (pp.id.subtract(temp).add(Library.upperBound)).mod(Library.upperBound);
				BigInteger preId = ((PeerProtocol)pp.predecessor.getProtocol(pid)).id;
				pp.updateMaxFwdTime(Library.sendOverhead);
				if ((bi.compareTo(pp.id) <= 0 && bi.compareTo(preId) > 0) || 
						(preId.compareTo(pp.id) > 0 && (bi.compareTo(preId) > 0 
								|| bi.compareTo(pp.id) <= 0)))
				{
					EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), new UFTMessage(type, i, 
							newNode.getIndex(), pp.succList[0].getIndex()), pp.predecessor, pid);
				}
				else
				{
					Node nextNode = pp.closetPreNode(newNode, bi);
					if (nextNode != null)
					{
						pp.messCount++;
						String messageId = pp.id.toString() + " " + Long.toString(pp.messCount);
						EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), new LookUpSuccMessage(
					messageId, type, i, bi, newNode, 0, nextNode.getIndex()), nextNode, pid);
					}
				}
			}
			if (Library.type == 9)
			{
				if (!type)
				{
					pp.updateMaxFwdTime(Library.sendOverhead);
					LeaveNotifyMessage lnm = new LeaveNotifyMessage(
							newNode.getIndex(), pp.predecessor.getIndex());
					EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), lnm, pp.succList[0], pid);
					pp.updateMaxFwdTime(Library.sendOverhead);
					ReplicaModifyMessage rmm = new ReplicaModifyMessage(
							newNode.getIndex(), pp.replicaIds);
					EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), rmm, pp.predecessor, pid);
				}
				else
				{
					pp.updateFinTable();
					pp.updateMaxFwdTime(Library.sendOverhead);
					JoinNotifyMessage jnm = new JoinNotifyMessage(newNode.getIndex());
					int sucId = (newNode.getIndex() + 1) % pp.numServer;
					PeerProtocol pp1 = (PeerProtocol)Network.get(sucId).getProtocol(pid);
					while(!pp1.isUp())
					{
						sucId++;
						sucId %= pp.numServer;
						pp1 = (PeerProtocol)Network.get(sucId).getProtocol(pid);
					}
					EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), jnm, Network.get(sucId), pid);
					pp.updateMaxFwdTime(Library.sendOverhead);
					EDSimulator.add(pp.waitTimeCal(pp.maxFwdTime), jnm, pp1.predecessor, pid);
				}
			}
		}
	}
}