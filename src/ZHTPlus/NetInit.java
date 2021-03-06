import java.math.BigInteger;
import java.sql.BatchUpdateException;
import java.util.*;

import peersim.core.*;
import peersim.config.Configuration;

public class NetInit implements Control {
	private static final String PAR_PROT = "protocol";
	private static final String PAR_NETSPEED = "netSpeed";
	private static final String PAR_LATENCY = "latency";
	private static final String PAR_MSGSIZE = "msgSize";
	private static final String PAR_PACKOVERHEAD = "packOverhead";
	private static final String PAR_UNPACKOVERHEAD = "unpackOverhead";
	private static final String PAR_NUMCLIENT = "numClient";
	private static final String PAR_NUMSERVER = "numServer";
	private static final String PAR_PROCTIME = "procTime";

	private static final String PAR_TIMETHRESHOLD = "timeThreshold";
	
	private static final String PAR_BATCHSIZE = "batchSize";
	private static final String PAR_SYSOVERHEAD = "sysOverhead";
	
	private int pid;
	private int numClient;
	private int numServer;
	//private int batchSize;
	private long timeThreshold;

	public NetInit(String prefix) {
		this.pid = Configuration.getPid(prefix + "." + PAR_PROT);
		this.numClient = Configuration.getInt(prefix + "." + PAR_NUMCLIENT);
		this.numServer = Configuration.getInt(prefix + "." + PAR_NUMSERVER);
		
		this.timeThreshold = Configuration.getLong(prefix + "." + PAR_TIMETHRESHOLD);
		Library.netSpeed = Configuration.getLong(prefix + "." + PAR_NETSPEED);
		Library.latency = Configuration.getLong(prefix + "." + PAR_LATENCY);
		Library.msgSize = Configuration.getLong(prefix + "." + PAR_MSGSIZE);
		Library.sendOverhead = Configuration.getLong(prefix + "." + PAR_PACKOVERHEAD);
		Library.recvOverhead = Configuration.getLong(prefix + "." + PAR_UNPACKOVERHEAD);
		Library.procTime = Configuration.getLong(prefix + "." + PAR_PROCTIME);
		
		Library.batchSize = Configuration.getInt(prefix + "." + PAR_BATCHSIZE);
		Library.sysOverhead = Configuration.getLong(prefix + "." + PAR_SYSOVERHEAD);
		//System.out.println("netinit = " + Library.batchSize);
		Library.initLib();
	}

	@SuppressWarnings("unchecked")
	public void initNode() {
		for (int i = 0; i < Network.size(); i++) {
			Node node = (Node) Network.get(i);
			PeerProtocol pp = (PeerProtocol) node.getProtocol(pid);
			pp.id = new BigInteger(Integer.toString(i));
			pp.numClient = numClient;
			pp.numServer = numServer;
			pp.maxTime = 0;
			pp.maxFwdTime = 0;
			pp.hmData = new HashMap<String, String>();
			pp.numAllReqFinished = 0;
			pp.numReqRecv = 0;
			pp.throughput = 0;
			pp.localTransTime = 0L;
			pp.clientThroughput = 0;
			pp.numReqSubmitted = 0;
			
			pp.batchSize = Library.batchSize; //batchSize;
			//System.out.println("pp.batchSize = " + Library.batchSize);
			pp.timeThreshold = timeThreshold;
			pp.batchVector = new ArrayList[numServer];
			pp.batchDeadline = new long[numServer];
			
			for (int j = 0; j < numServer; j++){
				pp.batchVector[j] = new ArrayList<OperaMessage>();
				pp.batchDeadline[j] = 10*CommonState.getTime(); //infinite time.
			}
			
		}
	}

	public boolean execute() {
		initNode();
		return false;
	}
}