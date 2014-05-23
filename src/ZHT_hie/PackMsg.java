import java.util.ArrayList;
import peersim.core.Node;

public class PackMsg {
	public Node sender;
	public Node dest;
	public ArrayList<GeneralMsgEvent> msgList; 
	
	public PackMsg(Node sender, Node dest){
		this.sender = sender;
		this.dest = dest;
		this.msgList = new ArrayList<GeneralMsgEvent>();
	}
	
	public int add(GeneralMsgEvent msg){
		this.msgList.add(msg);
		return this.msgList.size();
	}
	public int size(){
		return this.msgList.size();
	}
	
}
