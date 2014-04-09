public class ResClientMessage 
{
	public long taskId;
	public int type;
	public String key;
	public String value;
	public boolean success;
	public boolean cur;
	public int numHops;
	
	public ResClientMessage(long taskId, int type, String key, 
			String value, boolean success, boolean cur, int numHops)
	{
		this.taskId = taskId;
		this.type = type;
		this.key = key;
		this.value = value;
		this.success = success;
		this.cur = cur;
		this.numHops = numHops;
	}
}