import peersim.core.Node;

public class DFCForwardMessage 
{
	public long taskId;
	public Node source;
	public int intermid;
	public int type;
	public String key;
	public String value;
	public int numHops;
	public boolean queued;
	
	public DFCForwardMessage(long taskId, Node source,  int intermid, 
			int type, String key, String value, int numHops, boolean queued)
	{
		this.taskId = taskId;
		this.source = source;
		this.intermid = intermid;
		this.type = type;
		this.key = key;
		this.value = value;
		this.numHops = numHops;
		this.queued = queued;
	}
}