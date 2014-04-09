public class ReplicaModifyMessage 
{
	int leaveId;
	int[] replicaId;
	
	public ReplicaModifyMessage(int leaveId, int[] replicaId)
	{
		this.leaveId = leaveId;
		this.replicaId = new int[replicaId.length];
		for (int i = 0;i < replicaId.length;i++)
		{
			this.replicaId[i] = replicaId[i];
		}
	}
}