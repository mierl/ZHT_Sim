public class GenericObj 
{
	public QuorumKey qk;
	public int operaType = 0;
	public String key = null;
	public String value = null;

	public GenericObj(QuorumKey qk, int operaType, String key, String value)
	{
		this.qk = qk;
		this.operaType = operaType;
		this.key = key;
		this.value = value;
	}
}
