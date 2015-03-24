import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

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

	//public static String batchPolicy;
	public static String batchPolicy;
	public static int qosPattern;
	public static int batchSize;
	public static long sysOverhead;
	public static HashMap<Long, TaskDetail> taskHM;

	public static void initLib() {

//		if (Network.size() >= 1) {
//			Library.latency = 100*Library.latency
//					* (long) (Math.log10(Network.size()/4) / Math.log10(2))
//					* (long) (Math.sqrt((Network.size()/4) / 1024) + 2);
//		}

		Library.commOverhead = Library.msgSize * 8L * 1000000L
				/ Library.netSpeed + Library.latency;

		Library.numOperaFinished = 0;

		Library.numAllMessage = 0;

		Library.taskId = 0;
		Library.taskHM = new HashMap<Long, TaskDetail>();
	}

	/* calculate the throughput of each server */
	public static void logServer(int pid) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter("./server_"
					+ Network.size() + ".txt"));
			bw.write("ServerId\tNumTaskFinished\tThroughput\n");
			int numClient = ((PeerProtocol) (Network.get(0).getProtocol(pid))).numClient;
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
			BufferedWriter bw = new BufferedWriter(new FileWriter("./task_"
					+ Network.size() + ".txt"));
			bw.write("TaskId\tSubmitTime\tSentTime\tQueuedTime\tEndTime\tBackTime\n");
			for (long i = 1; i <= Library.numAllOpera; i++) {
				TaskDetail td = Library.taskHM.get(i);
				bw.write(i + "\t" + td.taskSubmitTime + "\t" + td.taskSentTime
						+ "\t" + td.taskQueuedTime + "\t" + td.taskEndTime
						+ "\t" + td.taskBackClientTime + "\n");
			}
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
