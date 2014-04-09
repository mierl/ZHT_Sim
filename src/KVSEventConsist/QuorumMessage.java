public class QuorumMessage
{
	QuorumKey qk;
	int senderId;
	String key;
	Versioned versioned;
	int type;
	
	public QuorumMessage(QuorumKey qk, int senderId, String key, Versioned versioned, int type)
	{
		this.qk = qk;
		this.senderId = senderId;
		this.key = key;
		this.versioned = versioned;
		this.type = type;
	}
}
