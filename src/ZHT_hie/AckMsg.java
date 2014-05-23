import peersim.core.Node;
public class AckMsg extends GeneralMsgEvent{
	public long messageId;
	public int state;
	public Node dest;
	public AckMsg(long messageId, Node initialSender, Node currentSende, int state) {
		super(0, initialSender, currentSende, 9, null, null);
		this.messageId = messageId;
		this.state = state;
	}
}
