package testProjectDB2;

import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.io.*;
import java.util.*;

public class WaitQueue
{
	private String item;
	private String transID;
	private LOCK l;
	
	public WaitQueue()
	{
		item = null;
		transID = null;
		l = LOCK.READLOCK;
	}
	public void setItem(String item)
	{
		this.item = item;
	}
	public String getItem()
	{
		return item;
	}
	public void setTransID(String id)
	{
		transID = id;
	}
	public String getTransID()
	{
		return transID;
	}
	public void setItemLock(LOCK lock)
	{
		l = lock;
	}
	public LOCK getItemLock()
	{
		return l;
	}
	
	public static void addToQueue(String item, Hashtable waitTable, Queue waitQueue)
	{
		Iterator<String> it = waitQueue.iterator();
		while(it.hasNext())
		{
			waitQueue.add(it.next());
		}
		
	}

}
