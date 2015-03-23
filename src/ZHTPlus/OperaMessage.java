import peersim.core.Node;

public class OperaMessage {
	public long taskId;
	public Node sender;
	public int type;
	public String key;
	public String value;
	public long QoS;

	public OperaMessage(long taskId, Node sender, int type, String key,
			String value, long qos) {
		this.taskId = taskId;
		this.sender = sender;
		this.type = type;
		this.key = key;
		this.value = value;
		this.QoS = qos;
	}
}

class BatchMessage {
	Node sender;
	OperaMessage[] requests;
}
