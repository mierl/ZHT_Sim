import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import peersim.core.*;
import peersim.config.Configuration;

public class TrafficGene implements Control
{
	private static final String PAR_PROT = "protocol";
	private static final String PAR_IDLENGTH = "idLength";
	
	private final int pid;
	private final int idLength;
	
	public TrafficGene(String prefix)
	{
		pid = Configuration.getPid(prefix + "." + PAR_PROT);
		idLength = Configuration.getInt(prefix + "." + PAR_IDLENGTH);
	}
	
	//System defined, implement
	public boolean execute()
	{
		int size = Network.size();
		String path = "./startup/startup_file_" + Integer.toString(size) + ".txt";
		BufferedReader br = null;
		try
		{
			br = new BufferedReader(new FileReader(path));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		TreeSet<Comparator> ts = new TreeSet<Comparator>();
		try
		{
			for (int i = 0;i < size;i++)
			{
				long wait = Long.parseLong(br.readLine());
				Comparator comp = new Comparator(wait, i);
				ts.add(comp);
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		Iterator<Comparator> it = ts.iterator();
		while (it.hasNext())
		{
			Comparator comp = it.next();
			Node node = (Node)Network.get(comp.id);
			PeerProtocol pp = (PeerProtocol)node.getProtocol(pid);
			pp.doRequest(node, idLength, pid, comp.wait);
		}
		return false;
	}
}