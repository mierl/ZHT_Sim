public class TaskDetail {
	public long taskId;
	public int clientId;
	public String taskSuccess;
	public String taskType;
	public long taskSubmitTime;
	public long taskSentTime;
	public long taskQueuedTime;
	public long taskEndTime;
	public long taskBackClientTime;
	public int numHops;

	public TaskDetail(long taskId, int clientId, String taskSuccess,
			String taskType, long taskSubmitTime, long taskSentTime, long taskQueuedTime,
			long taskEndTime, long taskBackClientTime, int numHops) {
		this.taskId = taskId;
		this.clientId = clientId;
		this.taskSuccess = taskSuccess;
		this.taskType = taskType;
		this.taskSubmitTime = taskSubmitTime;
		this.taskQueuedTime = taskQueuedTime;
		this.taskEndTime = taskEndTime;
		this.taskBackClientTime = taskBackClientTime;
		this.numHops = numHops;
	}
}
