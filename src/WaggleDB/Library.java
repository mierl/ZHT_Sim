import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import java.util.*;
import java.io.*;

public class Library {
	public static long netSpeed;
	public static long latency;
	public static long msgSize;
	public static long commOverhead;
	public static long sendOverhead;
	public static long recvOverhead;

	public static long procTime;

	public static long numOperaFinished;
	public static long numAllMessage;
	public static long numAllOpera;

	public static long taskId;
	
	public static HashMap<Long, TaskDetail> taskHM;
	public static ArrayList<Double[]> throughputLogAL;
	
	public static long logInterval;
	public static void initLib() {
		if (Network.size() >= 1) {
			Library.latency = Library.latency
					* (long) (Math.log10(Network.size()) / Math.log10(2))
					* (long) (Math.sqrt(Network.size() / 1024) + 1.5);
		}
		Library.commOverhead = Library.msgSize * 8L * 1000000L
				/ Library.netSpeed + Library.latency;
	
		//Library.procTime = 400L;

		Library.numOperaFinished = 0;

		Library.numAllMessage = 0;

		//Library.numAllOpera = (long) Network.size() * (long) numOpera;

		Library.taskId = 0;
		
		Library.taskHM = new HashMap<Long, TaskDetail>();
	}

	/* calculate the throughput of each server */
	public static void logServer(int pid) {
		try {
			BufferedWriter bw = new BufferedWriter(
					new FileWriter("./server_" + Network.size() + ".txt"));
			bw.write("ServerId\tNumTaskFinished\tThroughput\n");
			int numClient = ((PeerProtocol)(Network.get(0).getProtocol(pid))).numClient;
			int numServer = Network.size() - numClient;
			
			for (int i = 0; i < numServer; i++) {
				Node node = Network.get(i + numClient);
				PeerProtocol pp = (PeerProtocol) node.getProtocol(pid);
				pp.throughput = (double) pp.numReqRecv
						/ (double) (CommonState.getTime() + Library.recvOverhead)
						* 1E6;
				bw.write(i + "\t" + pp.numReqRecv + "\t" + pp.throughput + "\n");
			}
			
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void logTask() {
		try {
			BufferedWriter bw = new BufferedWriter(
					new FileWriter("./task_" + Network.size() + ".txt"));
			bw.write("TaskId\tSubmitTime\tQueuedTime\tEndTime\tBackTime\n");
			for (long i = 1; i <= Library.numAllOpera; i++) {
				TaskDetail td = Library.taskHM.get(i);
				bw.write(i + "\t" + td.taskSubmitTime + "\t" + td.taskQueuedTime + "\t" + 
						td.taskEndTime + "\t" + td.taskBackClientTime + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void logRealTimeThroughput() {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter("./real_time_throughput.txt"));
			for (int i = 0; i < Library.throughputLogAL.size(); i++) {
				Double[] throghputArray = Library.throughputLogAL.get(i);
				double sum = 0.0;
				for (int j = 0; j < throghputArray.length; j++) {
					sum += throghputArray[j];
				}
				bw.write(sum + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
