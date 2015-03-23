public class ReplicaInfo {
	public OperaMessage om;
	public int numReplicaRecv;

	public ReplicaInfo(OperaMessage om, int numReplicaRecv) {
		this.om = om;
		this.numReplicaRecv = numReplicaRecv;
	}
}
