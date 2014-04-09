import peersim.core.Node;

public class DChordOperaMessage 
{
	public QuorumKey qk;
	public int type;
	public Node sender;
	public String key;
	public String value;
	public int servId;
	public int numHops;
	public boolean queued;
	
	public DChordOperaMessage(QuorumKey qk, int type, Node sender, String key, 
			String value, int servId, int numHops, boolean queued)
	{
		this.qk = qk;
		this.type = type;
		this.sender = sender;
		this.key = key;
		this.value = value;
		this.servId = servId;
		this.numHops = numHops;
		this.queued = queued;
	}
}
