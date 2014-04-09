import java.math.*;
import peersim.core.Node;

public class LookUpSuccMessage 
{
	public String messageId;
	public boolean type;
	public int i;
	public BigInteger key;
	public Node sender;
	public int numHops;
	public int distId;
	
	public LookUpSuccMessage(String messageId, boolean type, int i, BigInteger key, Node sender, int numHops, int distId)
	{
		this.messageId = messageId;
		this.type = type;
		this.i = i;
		this.key = key;
		this.sender = sender;
		this.numHops = numHops;
		this.distId = distId;
	}
}