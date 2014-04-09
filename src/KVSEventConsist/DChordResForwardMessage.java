import peersim.core.Node;

public class DChordResForwardMessage 
{
	public long taskId;
	public String messageId;
	public int type;
	public String key;
	public String value;
	public int numHops;
	public Node target;
	public boolean success;
	
	public DChordResForwardMessage(long taskId, String messageId, int type, 
							String key, String value, int numHops, Node target, boolean success)
	{
		this.taskId = taskId;
		this.messageId = messageId;
		this.type = type;
		this.key = key;
		this.value = value;
		this.numHops = numHops;
		this.target = target;
		this.success = success;
	}
}