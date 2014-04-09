public class WaitingEvent 
{
	public int toNode;
	public long startTime;
	public long endTime;
	public long extra;
	public Object event;
	
	public WaitingEvent(int toNode, long startTime, long endTime, long extra, Object event)
	{
		this.toNode = toNode;
		this.startTime = startTime;
		this.endTime = endTime;
		this.extra = extra;
		this.event = event;
	}
}
