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
	
	public boolean execute()
	{
		int size = Network.size();
		int numClient = Network.size() - Library.offset;
		String path = "./startup/startup_file_" + Integer.toString(numClient) + ".txt";
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
			for (int i = Library.offset;i < size;i++)
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
			Node server = (Node)Network.get(((PeerProtocol)node.getProtocol(pid)).servId);
			pp.doRequest(node, server, idLength, pid, comp.wait);
			if (comp.id == Library.offset)
			{
				Library.logTask(comp.id, true, pid, comp.wait + Library.clientSendOverhead);
			}
		}
		return false;
	}
}