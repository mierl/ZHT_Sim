import peersim.core.Node;
import java.math.*;

public class ResLookUpMessage 
{
	public String messageId;
	public boolean type;
    public int i;
	public BigInteger key;
	public int numHops;
	public Node target;
	public boolean success;
	
	public ResLookUpMessage(String messageId, boolean type, int i, BigInteger key, 
			int numHops, Node target, boolean success)
	{
		this.messageId = messageId;
		this.type = type;
		this.i = i;
		this.key = key;
		this.numHops = numHops;
		this.target = target;
		this.success = success;
	}
}
