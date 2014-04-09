import java.util.Comparator;
import java.math.*;
import peersim.core.*;

public class NodeComparator implements Comparator<Object>
{
	public int pid = 0;
	
	public NodeComparator(int pid)
	{
		this.pid = pid;
	}
	
	public int compare(Object o1, Object o2)
	{
		BigInteger b1 = ((PeerProtocol)((Node)o1).getProtocol(pid)).id;
		BigInteger b2 = ((PeerProtocol)((Node)o2).getProtocol(pid)).id;
		
		if (b2.compareTo(BigInteger.ZERO) < 0)
		{
			return 1;
		}
		else if(b1.compareTo(BigInteger.ONE) < 0)
		{
			return 1;
		}
		else
		{
			return b1.compareTo(b2);
		}
	}
}
