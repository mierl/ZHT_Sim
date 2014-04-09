import java.util.*;

public class ResReloadDataMessage 
{
	public HashMap<String, String> hmData;
	
	public ResReloadDataMessage(HashMap<String, String> hmData)
	{
		this.hmData = new HashMap<String, String>();
		this.hmData = hmData;
	}
}
