import peersim.core.Node;

public class DChordOperaMessage 
{
	public long taskId;
	public int type;
	public Node sender;
	public String key;
	public String value;
	public int servId;
	public int numHops;
	public boolean queued;
	
	public DChordOperaMessage(long taskId, int type, Node sender, String key, 
			String value, int servId, int numHops, boolean queued)
	{
		this.taskId = taskId;
		this.type = type;
		this.sender = sender;
		this.key = key;
		this.value = value;
		this.servId = servId;
		this.numHops = numHops;
		this.queued = queued;
	}
}
