import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

public class Library 
{
	public static long netSpeed;
	public static long latency;
	public static long messSize;
	public static long commOverhead;
	public static long sendOverhead;
	public static long recvOverhead;

	public static long procTime;
	
	public static long numOperaFinished;
	public static long numAllMessage;
	public static long numAllOpera;
	
	public static long taskId;

	public static void initLib(int numOpera)
	{
		Library.netSpeed = 6800000000L;
		Library.latency = 5L;
		if (Network.size() >= 1)
		{
			Library.latency = Library.latency * (long)(Math.log10(Network.size()) / Math.log10(2)) * (long)(Math.sqrt(Network.size() / 1024) + 1.5);
		}
		Library.messSize = 134L;
		Library.commOverhead = Library.messSize * 8L * 1000000L 
				/ Library.netSpeed + Library.latency;
		System.out.println(Library.commOverhead);
		Library.sendOverhead = Library.recvOverhead = 3L;
		
		Library.procTime = 400L;
		
		Library.numOperaFinished = 0;
		
		Library.numAllMessage = 0;
		
		Library.numAllOpera = (long)Network.size() * (long)numOpera;
		
		Library.taskId = 0;
	}
	
	/* calculate the throughput of each server */
	public static void calServThroughput(int pid)
	{
		for (int i = 0;i < Network.size();i++)
		{
			Node node = Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			pp.throughput = (double)pp.numReqRecv / (double)
					(CommonState.getTime() + Library.recvOverhead) * 1E6;
		}
	}
}
