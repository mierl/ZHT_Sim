import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import peersim.core.*;
import peersim.config.Configuration;

public class WorkloadGen implements Control {
	private static final String PAR_PROT = "protocol";
	private static final String PAR_IDLENGTH = "idLength";
	private static final String PAR_BRANCH = "branch";
	private final int pid;
	private final int keyLength;
	private final int branch;

	public WorkloadGen(String prefix) {
		pid = Configuration.getPid(prefix + "." + PAR_PROT);
		keyLength = Configuration.getInt(prefix + "." + PAR_IDLENGTH);
		branch = Configuration.getInt(prefix + "." + PAR_BRANCH);
	}

	// System defined, implement
	public boolean execute() {
		System.out.println("WorkloadGen start.");

		int size = Network.size();
		String path = "./startup/startup_file_" + Integer.toString(size)
				+ ".txt";
		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(path));
		} catch (IOException e) {
			e.printStackTrace();
		}

		TreeSet<Comparator> ts = new TreeSet<Comparator>();

		try {
			for (int i = 0; i < size; i++) {
				long wait = Long.parseLong(br.readLine());
				Comparator comp = new Comparator(wait, i);// i for id
				ts.add(comp);
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Iterator<Comparator> it = ts.iterator();
		// 只是把所有的节点遍历一遍, 每个都发个请求而已.
		// TreeSet这里只是作为任意容器使用, 并无使用数据结构的特性
		// Comparator也没有用到内部方法, 只做为struct用
		while (it.hasNext()) {
			Comparator comp = it.next();
			Node sender = (Node) Network.get(comp.id);
			// get it but didn't do anything to node??
			// System.out.println("sender id = " + sender.getID());

			PeerProtocol pp = (PeerProtocol) sender.getProtocol(pid);
			// System.out.println("pp id = " + pp.id);
			// pp.roles.listRoles();

			// get peer from node
			if (pp.roles.isClient) {
				if(0 == branch){//orginal code
					pp.doRequest(sender, keyLength, pid, comp.wait); //orginal
				}else if(1 == branch){//ZHT-H
					pp.initRequest(sender, keyLength, pid, comp.wait); 
				}
				
				
			}
			// for each peer, do requests
		}
		System.out.println("WorkloadGen end.");

		return false;
	}
}