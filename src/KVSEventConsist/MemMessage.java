import java.util.*;

public class MemMessage 
{
	public boolean type;
	public HashMap<Integer, Boolean> memList;
	public int distId;
	public boolean success;
	
	public MemMessage(boolean type, HashMap<Integer, Boolean> memList, int distId, boolean success)
	{
		this.type = type;
		this.memList = new HashMap<Integer, Boolean>();
		if (memList != null)
		{
			Iterator<Integer> it = memList.keySet().iterator();
			while (it.hasNext())
			{
				this.memList.put(it.next(), null);
			}
		}
		this.distId = distId;
		this.success = success;
	}
}
