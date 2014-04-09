import peersim.core.Node;

public class DChordForwardMessage 
{
	public QuorumKey qk;
	public String messageId;
	public int type;
	public Node sender;
	public String key;
	public String value;
	public int numHops;
	
	public DChordForwardMessage(QuorumKey qk, String messageId, int type, Node sender, 
				String key, String value, int numHops)
	{
		this.qk = qk;
		this.messageId = messageId;
		this.type = type;
		this.sender = sender;
		this.key = key;
		this.value = value;
		this.numHops = numHops;
	}
}
