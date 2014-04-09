import peersim.core.Node;

public class DFCResForwardMessage 
{
	public long taskId;
	public Node source;
	public int type;
	public String key;
	public String value;
	public boolean success;
	public int numHops;
	public int servId;
	
	public DFCResForwardMessage(long taskId, Node source, int type, 
			String key, String value, boolean success, int numHops, int servId)
	{
		this.taskId = taskId;
		this.source = source;
		this.type = type;
		this.key = key;
		this.value = value;
		this.success = success;
		this.numHops = numHops;
		this.servId = servId;
	}
}
