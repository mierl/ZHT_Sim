import java.math.BigInteger;
import java.util.*;
import peersim.core.*;
import peersim.config.Configuration;
import peersim.edsim.EDSimulator;

public class NetInit implements Control 
{
	private static final String PAR_PROT = "protocol";
	private static final String PAR_TYPE = "type";
	private static final String PAR_NUMSERVER = "numServer";
	private static final String PAR_NUMOPERA = "numOperation";
	private static final String PAR_NUMCLIENTPERSERV = "numClientPerServ";
	private static final String PAR_NUMREPLICA = "numReplica";
	private static final String PAR_IDLENGTH = "idLength";
	private static final String PAR_SUCCSIZE = "succListSize";
	private static final String PAR_CHURNINTERVAL = "churnInterval";
	//private static final String PAR_MAXNUMTRY = "maxNumTry";
	
	private int pid;
	private int numServer;
	private int numOpera;
	private int numClientPerServ;
	private int numReplica;
	private int idLength;
	private int succListSize;
	//private int maxNumTry;
	
	public NetInit(String prefix)
	{
		this.pid = Configuration.getPid(prefix + "." + PAR_PROT);
		Library.type = Configuration.getInt(prefix + "." + PAR_TYPE);
		this.succListSize = Configuration.getInt(prefix + "." + PAR_SUCCSIZE);
		this.numServer = Configuration.getInt(prefix + "." + PAR_NUMSERVER); 
		this.numOpera = Configuration.getInt(prefix + "." + PAR_NUMOPERA);
		this.numClientPerServ = Configuration.getInt(prefix + "." + PAR_NUMCLIENTPERSERV);
		this.numReplica = Configuration.getInt(prefix + "." + PAR_NUMREPLICA);
		this.idLength = Configuration.getInt(prefix + "." + PAR_IDLENGTH);
		this.succListSize = Configuration.getInt(prefix + "." + PAR_SUCCSIZE);
		Library.churnInterval = Configuration.getLong(prefix + "." + 
				PAR_CHURNINTERVAL);
		//this.maxNumTry = Configuration.getInt(prefix + "." + PAR_MAXNUMTRY);
	}
	
	public void dfcInit()
	{
		for (int i = 0;i < Network.size();i++)
		{
			Node node = (Node)Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			pp.id = new BigInteger(Integer.toString(i));
			if (i >= numServer)
			{
				pp.servId = (i - numServer) / numClientPerServ;
			}
			if (i < numServer)
			{
				pp.memList = new HashMap<Integer, Boolean>();
				for (int j = 0;j < numServer;j++)
				{
					pp.memList.put(j, null);
				}
			}
			pp.first = true;
			pp.setUp();
		}
	}
	
	public void dchordInit()
	{
		if (numServer < numReplica + 1)
		{
			System.out.println("Your number of replicas is more " +
					"than the number of servers, please check!");
			System.exit(1);
		}
		if (succListSize < numReplica)
		{
			System.out.println("Your number of replicas is more " +
					"than the number of successors, please check!");
			System.exit(1);
		}
		BigInteger range = Library.upperBound.divide(new BigInteger(Integer.toString(numServer))); 
		for (int i = 0;i < numServer;i++)
		{
			Node node = (Node)Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			pp.succListSize = succListSize;
			pp.id = range.multiply(new BigInteger(Integer.toString(i)));
			pp.fingerTable = new NodeExtend[idLength];
			pp.succList = new Node[succListSize];
			pp.setUp();
		}
		for (int i = numServer;i < Network.size();i++)
		{
			Node node = (Node)Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			pp.id = new BigInteger(Integer.toString(-1 * i));
		}
		NodeComparator nc = new NodeComparator(pid);
		Network.sort(nc);
		for (int i = numServer;i < Network.size();i++)
		{
			Node node = (Node)Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			pp.servId = (i - numServer) / numClientPerServ;
			pp.setUp();
		}
		InitSucPrec();
		createFingerTable();
	}
	
	public void overallInit()
	{
		for (int i = 0;i < Network.size();i++)
		{
			Node node = (Node)Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			if (i < Library.offset)
			{
				pp.maxTime = 0;
				pp.maxFwdTime = 0;
				pp.hmUpStreams = new HashMap<String, Integer>();
				pp.replicaIds = new int[numReplica];
				pp.versionedData = new HashMap<Integer, HashMap<String, Versioned>>();
				pp.quorumStore = new HashMap<QuorumKey, QuorumStore>();
				for (int j = 0;j < numReplica;j++)
				{
					if (Library.type == 10)
					{
						pp.replicaIds[j] = pp.getReplicaId(node, j);
					}
					else if (Library.type == 11)
					{
						pp.replicaIds[j] = pp.succList[j].getIndex();
					}
				}
				for (int j = 0;j <= numReplica;j++)
				{
					pp.versionedData.put((i - (numReplica - j) + numServer) % numServer,
											new HashMap<String, Versioned>());
				}
				pp.numFwdMsg = 0;
				pp.numFwdMsgFailed = 0;
				pp.numFwdReq = 0;
				pp.numFwdReqFailed = 0;
				pp.numRecFromOtherMsg = 0;
				pp.numRecFromOtherMsgFailed = 0;
				pp.numRecReqFromOther = 0;
				pp.numRecReqFromOtherFailed = 0;
				pp.numOtherReqFinished = 0;
				pp.numOtherReqFailed = 0;
				pp.numAllReqFinished = 0;
				pp.numAllReqFailed = 0;
				pp.numTaskSubmitted = 0;
				pp.throughput = 0;
			}
			pp.numLocalReq = 0;
			pp.numLocalReqFinished = 0;
			pp.numLocalReqFailed = 0;
			pp.hmTry = new HashMap<Long, Integer>();
			pp.clientThroughput = 0;
			pp.ready = true;
		}
	}
	
	public boolean execute()
	{
		Library.upperBound = BigInteger.ONE;
		for (int i = 0;i < idLength;i++)
		{
			Library.upperBound = Library.upperBound.multiply(BigInteger.valueOf(2));
		}
		switch (Library.type)
		{
			case 10:
				dfcInit();
				break;
			case 11:
				dchordInit();
				break;
		}
		Library.initLib(numServer, numOpera, numReplica, numClientPerServ);
		overallInit();
		EDSimulator.add(0, new LogMessage(0), Network.get(0), pid);
		Node node = Library.selChurnNode(numServer);
		EDSimulator.add(Library.currChurnTime, new ChurnNoticeMessage(), node, pid);
		return false;
	}
	
	public void InitSucPrec()
	{
		for (int i = 0;i < numServer;i++)
		{
			Node node = (Node)Network.get(i);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			for (int j = 0;j < succListSize;j++)
			{
				pp.succList[j] = Network.get((i + j + 1) % numServer);
			}
			if (i > 0)
			{
				pp.predecessor = (Node)Network.get(i - 1);
			}
			else
			{
				pp.predecessor = (Node)Network.get(numServer - 1);
			}
		}
	}
	
	public void createFingerTable()
	{
		BigInteger idFirst = ((PeerProtocol)Network.get(0).getProtocol(pid)).id;
		BigInteger idLast = ((PeerProtocol)Network.get(numServer - 1).getProtocol(pid)).id;
		for (int i = 0;i < numServer;i++)
		{
			Node node = (Node)Network.get(i);
			PeerProtocol cp = (PeerProtocol)node.getProtocol(pid);
			for (int j = 0;j < idLength;j++)
			{
				long dist = (long)(Math.pow(2, j));
				BigInteger pos = cp.id.add(BigInteger.valueOf(dist));
				pos = pos.mod(Library.upperBound);
				if (pos.compareTo(idLast) > 0 || pos.compareTo(idFirst) < 0)
				{
					cp.fingerTable[j] = new NodeExtend(Network.get(0), true);
				}
				else  
				{
					cp.fingerTable[j] = new NodeExtend(findSuccNode(pos, 0, numServer - 1), true);
				}
			}
		}
	}
	
	public Node findSuccNode(BigInteger id, int low, int high)
	{
		if (low >= high)
		{
			return Network.get(low);
		}
		int mid = (low + high) / 2;
		if (mid <= 0)
		{
			if (id.compareTo(((PeerProtocol)Network.get(low).getProtocol(pid)).id) == 0)
			{
				return Network.get(low);
			}
			else
			{
				return Network.get(high);
			}
		}
		else
		{
			BigInteger midId = ((PeerProtocol)Network.get(mid).getProtocol(pid)).id;
			BigInteger lowId = ((PeerProtocol)Network.get(mid - 1).getProtocol(pid)).id;
			BigInteger highId = ((PeerProtocol)Network.get(mid + 1).getProtocol(pid)).id;
			if (id.compareTo(midId) <= 0 && id.compareTo(lowId) > 0)
			{
				return Network.get(mid);
			}
			if (id.compareTo(highId) <= 0 && id.compareTo(midId) > 0)
			{
				return Network.get(mid + 1);
			}
			if (id.compareTo(midId) < 0)
			{
				return findSuccNode(id, low, mid);
			}
			else if (id.compareTo(midId) > 0)
			{
				return findSuccNode(id, mid, high);
			}
		}
		return null;
	}
}