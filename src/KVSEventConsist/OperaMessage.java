import peersim.core.Node;

public class OperaMessage 
{
	public QuorumKey qk;
	public Node sender;
	public int type;
	public String key;
	public String value;
	public int originId;
	public boolean queued;
	
	public OperaMessage(QuorumKey qk, Node sender, int type, String key, 
			String value, int originId, boolean queued)
	{
		this.qk = qk;
		this.sender = sender;
		this.type = type;
		this.key = key;
		this.value = value;
		this.originId = originId;
		this.queued = queued;
	}
}
