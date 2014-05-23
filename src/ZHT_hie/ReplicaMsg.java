import peersim.core.Node;

public class ReplicaMsg 
{
	public Node sender;
	public long messageId;
	public String key;
	public String value;
	
	public ReplicaMsg(Node sender, long messageId, String key, String value)
	{
		this.sender = sender;
		this.messageId = messageId;
		this.key = key;
		this.value = value;
	}
}
