import java.util.*;

public class QuorumStore 
{
	Object obj;
	int sucTime;
	int allTime;
	ArrayList<QuorumResMessage> al;
	
	public QuorumStore (Object obj, int sucTime, int allTime, ArrayList<QuorumResMessage> al)
	{
		this.obj = obj;
		this.sucTime = sucTime;
		this.allTime = allTime;
		this.al = al;
	}
}
