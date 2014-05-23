
public class ResReplicaMsg 
{
	public long messageId;
	public boolean success;
	
	public ResReplicaMsg(long messageId, boolean success)
	{
		this.messageId = messageId;
		this.success = success;
	}
}
