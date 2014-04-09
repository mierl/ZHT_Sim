import peersim.core.Node;

public class DChordForwardMessage 
{
	public long taskId;
	public String messageId;
	public int type;
	public Node sender;
	public String key;
	public String value;
	public int numHops;
	
	public DChordForwardMessage(long taskId, String messageId, int type, Node sender, 
				String key, String value, int numHops)
	{
		this.taskId = taskId;
		this.messageId = messageId;
		this.type = type;
		this.sender = sender;
		this.key = key;
		this.value = value;
		this.numHops = numHops;
	}
}
