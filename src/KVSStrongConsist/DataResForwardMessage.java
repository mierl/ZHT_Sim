public class DataResForwardMessage 
{
	public int senderId;
	public boolean success;
	public int[] myReplicas;
	
	public DataResForwardMessage(int senderId, boolean success, int[] myReplicas)
	{
		this.senderId = senderId;
		this.success = success;
		this.myReplicas = new int[myReplicas.length];
		for (int i = 0;i < myReplicas.length;i++)
		{
			this.myReplicas[i] = myReplicas[i];
		}
	}
}
