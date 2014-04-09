import java.util.*;

public class CTreeForwardMessage 
{
	public int senderId;
	public ArrayList<OperaMessage> al;
	
	public CTreeForwardMessage(int senderId, ArrayList<OperaMessage> al)
	{
		this.senderId = senderId;
		this.al = new ArrayList<OperaMessage>();
		for (int i = 0;i < al.size();i++)
		{
			this.al.add(al.get(i));
		}
	}
}