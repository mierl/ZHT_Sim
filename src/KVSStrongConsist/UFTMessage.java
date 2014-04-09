public class UFTMessage 
{
	boolean type;
	int i;
	int curId; 
	int sucId;
	
	public UFTMessage(boolean type, int i, int curId, int sucId)
	{
		this.type = type;
		this.i = i;
		this.curId = curId;
		this.sucId = sucId;
	}
}