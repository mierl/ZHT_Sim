import peersim.core.Node;


public class ClientReq extends GeneralMsgEvent{

	public ClientReq(long taskId, Node initialSender, Node currentSende, int type, String key,
			String value) {
		super(taskId, initialSender, currentSende, type, key, value);
		// TODO Auto-generated constructor stub
	}

}
