public class ResReplicaMessage {
	public long messageId;
	public boolean success;

	public ResReplicaMessage(long messageId, boolean success) {
		this.messageId = messageId;
		this.success = success;
	}
}
