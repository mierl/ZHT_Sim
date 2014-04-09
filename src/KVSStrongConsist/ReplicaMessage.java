import peersim.core.Node;

public class ReplicaMessage 
{
	public Node sender;
	public long messageId;
	public String key;
	public String value;
	
	public ReplicaMessage(Node sender, long messageId, String key, String value)
	{
		this.sender = sender;
		this.messageId = messageId;
		this.key = key;
		this.value = value;
	}
}
