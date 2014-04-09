public class ResClientMessageChurn 
{
	public long taskId;
	public int type;
	public String key;
	public String value;
	public boolean success;
	public boolean cur;
	public int numHops;
	public int curId;
	public int firstReplicaId; 
	
	public ResClientMessageChurn(long taskId, int type, String key, 
			String value, boolean success, boolean cur, int numHops, int curId, int firstReplicaId)
	{
		this.taskId = taskId;
		this.type = type;
		this.key = key;
		this.value = value;
		this.success = success;
		this.cur = cur;
		this.numHops = numHops;
		this.curId = curId;
		this.firstReplicaId = firstReplicaId;
	}
}
