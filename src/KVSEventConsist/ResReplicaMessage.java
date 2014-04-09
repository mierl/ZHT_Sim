
public class ResReplicaMessage 
{
	public long messageId;
	public boolean success;
	public int curRepId;
	
	public ResReplicaMessage(long messageId, boolean success, int curRepId)
	{
		this.messageId = messageId;
		this.success = success;
		this.curRepId = curRepId;
	}
}
