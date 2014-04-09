public class GenericObj 
{
	public long taskId;
	public int operaType = 0;
	public String key = null;
	public String value = null;

	public GenericObj(long taskId, int operaType, String key, String value)
	{
		this.taskId = taskId;
		this.operaType = operaType;
		this.key = key;
		this.value = value;
	}
}
