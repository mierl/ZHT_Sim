public class ReplicaInfo 
{
	public OperationMsg msg;
	public int numReplicaRecv;
	
	public ReplicaInfo(OperationMsg om, int numReplicaRecv)
	{
		this.msg = om;
		this.numReplicaRecv = numReplicaRecv;
	}
}
