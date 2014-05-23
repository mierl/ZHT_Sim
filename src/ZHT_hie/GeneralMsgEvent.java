import peersim.core.CommonState;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class GeneralMsgEvent extends OperationMsg {

	public Node currentSender;
	public GeneralMsgEvent(long taskId, Node initialSender, Node currentSende, int type, String key,
			String value) {
		super(taskId, initialSender, type, key, value);
		this.currentSender = currentSende;
		// TODO Auto-generated constructor stub
	}

	/*
	 * public GeneralMsgEvent(long taskId, Node sender, int type, String key,
	 * String value) { this.taskId = taskId; this.sender = sender; this.type =
	 * type; this.key = key; this.value = value; }
	 */

	public int updateTime() {
		//
		return 0;
	}

	/*
	 * public int addToScheduler(Node node) { // if (node.getIndex() !=
	 * this.sender.getIndex()) {// ??? EDSimulator.add(waitTimeCal(maxFwdTime),
	 * responseMsg, this.sender, parameters.pid); } else {
	 * EDSimulator.add(maxFwdTime - CommonState.getTime(), responseMsg,
	 * opMsg.sender, parameters.pid); } return 0; }
	 */

}
