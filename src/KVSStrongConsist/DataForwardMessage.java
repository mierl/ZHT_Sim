import java.util.*;

public class DataForwardMessage 
{
	public int senderId;
	public HashMap<Integer, HashMap<String, String>> hmData;
	public boolean type;
	public int[] myReplicaIds;
	
	public DataForwardMessage(int senderId, HashMap<Integer, 
			HashMap<String, String>> hmData, boolean type, int[] myReplicaIds)
	{
		this.senderId = senderId;
		this.hmData = new HashMap<Integer, HashMap<String, String>>();
		Iterator<Integer> it = hmData.keySet().iterator();
		while (it.hasNext())
		{
			int id = it.next();
			this.hmData.put(id, hmData.get(id));
		}
		this.type = type;
		this.myReplicaIds = new int[myReplicaIds.length];
		for (int i = 0;i < myReplicaIds.length;i++)
		{
			this.myReplicaIds[i] = myReplicaIds[i];
		}
	}
}
