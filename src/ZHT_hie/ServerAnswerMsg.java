import peersim.core.Node;

public class ServerAnswerMsg extends GeneralMsgEvent {

	public int retState; // 0 for success;
	public int senderServerID;

	public ServerAnswerMsg(long taskId, Node initialSender, Node currentSende, int senderServerID,
			String key, String value, int retState) {
		super(taskId, initialSender, currentSende, 9, key, value);
		this.initialSender = initialSender;
		this.retState = retState;
	}

}
