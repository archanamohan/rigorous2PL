package testProjectDB2;

public class Locking
{
	private String item;
	private String trID;
	private LOCK lockState;
	public int read_counter;
	
	public Locking()
	{
		item = null;
		trID = null;
		lockState = LOCK.UNLOCK;
		read_counter = 0;
	}
	public void setItem(String item)
	{
		this.item = item;
	}
	public String getItem()
	{
		return item;
	}
	public void setTransID(String tId)
	{
		trID = tId;
	}
	public String getTransID()
	{
		return trID;
	}
	public void setLockState(LOCK lock)
	{
		lockState = lock;
	}
	public LOCK getLockState()
	{
		return lockState;
	}
}