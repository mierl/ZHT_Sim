public class QuorumResMessage 
{
	QuorumKey qk;
	int replicaId;
	Versioned versioned;
	boolean suc;
	
	public QuorumResMessage (QuorumKey qk, int replicaId, Versioned versioned, boolean suc)
	{
		this.qk = qk;
		this.replicaId = replicaId;
		this.versioned = versioned;
		this.suc = suc;
	}
}
