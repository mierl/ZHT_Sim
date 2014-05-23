import peersim.core.Node;

public class OperationMsg 
{
	public long taskId;
	public Node initialSender;
	public int type;
	public String key;
	public String value;
	
	public OperationMsg(long taskId, Node initialSender, int type, String key, String value)
	{
		this.taskId = taskId;
		this.initialSender = initialSender;
		this.type = type;
		this.key = key;
		this.value = value;
	}
}
