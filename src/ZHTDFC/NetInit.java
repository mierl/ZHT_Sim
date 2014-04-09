import java.math.BigInteger;
import java.util.*;
import peersim.core.*;
import peersim.config.Configuration;

public class NetInit implements Control 
{
	private static final String PAR_PROT = "protocol";
	private static final String PAR_NUMOPERA = "numOperation";
	
	private int pid;
	private int numOpera;
	
	public NetInit(String prefix)
	{
		this.pid = Configuration.getPid(prefix + "." + PAR_PROT);
		this.numOpera = Configuration.getInt(prefix + "." + PAR_NUMOPERA);
	}
	
	public void dfcInit()
	{
		for (int i = 0;i < Network.size();i++)
		{
			Node node = (Node)Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			pp.id = new BigInteger(Integer.toString(i));
		}
	}
	
	public void overallInit()
	{
		for (int i = 0;i < Network.size();i++)
		{
			Node node = (Node)Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			pp.maxTime = 0;
			pp.maxFwdTime = 0;
			pp.hmData = new HashMap<String, String>();
			pp.hmReplica = new HashMap<Long, ReplicaInfo>();
			pp.numFwdMsg = 0;
			pp.numAllReqFinished = 0;
			pp.numAllReqFinished = 0;
			pp.throughput = 0;
			pp.numReqRecv = 0;
			pp.clientThroughput = 0;
		}
	}
	
	public boolean execute()
	{
		dfcInit();
		overallInit();
		Library.initLib(numOpera);
		return false;
	}
}