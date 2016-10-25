package testProjectDB2;

public class Transaction
{
	public static int tsCounter = 0;
	
	private String transID;
	private int  timestamp;
	private STATE state;
	
	public Transaction()
	{
		transID = null;
		tsCounter=tsCounter + 1;
		timestamp = tsCounter;
		state= STATE.ACTIVE;		
	}
	public void setTransID(String tId)
	{
		transID = tId;
	}
	public String getTransID()
	{
		return transID;
	}
	public int getTimeStamp()
	{
		return timestamp;
	}
	public void setState(STATE s)
	{
		state = s;
	}
	public STATE getState()
	{
		return state;
	}
}