
public class NetChurnMessage 
{
	public boolean type;
	public int nodeId;
	public boolean notify;
	
	public NetChurnMessage(boolean type, int nodeId, boolean notify)
	{
		this.type = type;
		this.nodeId = nodeId;
		this.notify = notify;
	}
}
