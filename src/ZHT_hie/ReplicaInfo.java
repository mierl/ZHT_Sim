public class ReplicaInfo 
{
	public OperationMessage msg;
	public int numReplicaRecv;
	
	public ReplicaInfo(OperationMessage om, int numReplicaRecv)
	{
		this.msg = om;
		this.numReplicaRecv = numReplicaRecv;
	}
}
