import peersim.core.Node;

public class DChordResMessage 
{
	public long taskId;
	public int type;
	public Node sender;
	public String key;
	public String value;
	public boolean success;
	public boolean first;
	public int numHops;
	public int curId;
	public int firstReplicaId;
	
	public DChordResMessage(long taskId, int type, Node sender, String key, String value,
					boolean success, boolean first, int numHops, int curId, int firstReplicaId)
	{
		this.taskId = taskId;
		this.type = type;
		this.sender = sender;
		this.key = key;
		this.value = value;
		this.success = success;
		this.first = first;
		this.numHops = numHops;
		this.curId = curId;
		this.firstReplicaId = firstReplicaId;
	}
}