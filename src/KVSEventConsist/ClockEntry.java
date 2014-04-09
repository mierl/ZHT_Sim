public class ClockEntry 
{
	public int nodeId;
	public long version;
	
	public ClockEntry(int nodeId, long version)
	{
		this.nodeId = nodeId;
		this.version = version;
	}
	
	public ClockEntry incremented()
	{
		return new ClockEntry(nodeId, version + 1);
	}
}