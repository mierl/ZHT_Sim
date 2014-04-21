public class ReplicaInfo 
{
	public OperationMessage om;
	public int numReplicaRecv;
	
	public ReplicaInfo(OperationMessage om, int numReplicaRecv)
	{
		this.om = om;
		this.numReplicaRecv = numReplicaRecv;
	}
}
