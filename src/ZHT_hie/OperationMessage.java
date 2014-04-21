import peersim.core.Node;

public class OperationMessage 
{
	public long taskId;
	public Node sender;
	public int type;
	public String key;
	public String value;
	
	public OperationMessage(long taskId, Node sender, int type, String key, String value)
	{
		this.taskId = taskId;
		this.sender = sender;
		this.type = type;
		this.key = key;
		this.value = value;
	}
}
