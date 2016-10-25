package testProjectDB2;

import java.io.*;
import java.util.*;

enum STATE
{
	ACTIVE, BLOCKED, ABORTED, COMMITED
};

enum LOCK
{
	UNLOCK, READLOCK, WRITELOCK
};


public class fileReader
{
	/*We have made three global data structures for Transaction table, Lock table and Wait queue.
	 **/
	public static Hashtable<String, Transaction> TransactionTable = new Hashtable<String, Transaction>();
	public static Hashtable<String, Locking> LockingTable = new Hashtable<String, Locking>();
	public static LinkedHashMap<String, WaitQueue> WaitTable = new LinkedHashMap<String, WaitQueue>();
	
	
	public static void ReadOperation(String tId, String item)
	{
		if(TransactionTable.containsKey(tId) && TransactionTable.get(tId).getState() == STATE.ABORTED)
		{
			System.out.println("Transaction " + tId + " is ABORTED");
		}
		else
		{
			if(LockingTable.containsKey(item) == false)
			{
			Locking l= new Locking();
			l.setItem(item);
			l.setTransID(tId);
			l.setLockState(LOCK.READLOCK);
			l.read_counter++;
			LockingTable.put(item, l);
			}
			else
			{
				if(LockingTable.get(item).getLockState() == LOCK.UNLOCK)
				{
					LockingTable.get(item).setLockState(LOCK.READLOCK);
					LockingTable.get(item).setTransID(tId);
					LockingTable.get(item).read_counter++;
				}
				else if(LockingTable.get(item).getLockState() == LOCK.WRITELOCK)
				{
					String transactionID = LockingTable.get(item).getTransID();// Fetching the older transaction
					if( TransactionTable.get(tId).getTimeStamp() < TransactionTable.get(transactionID).getTimeStamp())
					{
						//Put tid on LinkedHashTable wala WaitTable 
						WaitQueue wq = new WaitQueue();
						wq.setItem(item);
						wq.setTransID(tId);
						wq.setItemLock(LOCK.READLOCK);
						WaitTable.put(item, wq);
						
						TransactionTable.get(tId).setState(STATE.BLOCKED);
					}
					else
					{
						fileReader.Unlock(tId);
						TransactionTable.get(tId).setState(STATE.ABORTED);
					}
				}
				else if(LockingTable.get(item).getLockState() == LOCK.READLOCK)
				{
					LockingTable.get(item).read_counter++;
					//Do concat for multiple reads
					LockingTable.get(item).setTransID(LockingTable.get(item).getTransID().concat(tId));
				}
			}
		}
	}
	
	/* Having a separate Write function*/
	public static void WriteOperation(String tId, String item)
	{
		if(TransactionTable.containsKey(tId) && TransactionTable.get(tId).getState() == STATE.ABORTED)
		{
			System.out.println("Transaction " + tId + " is ABORTED");
		}
		else
		{
			if(LockingTable.containsKey(item) == false)
			{
			Locking l= new Locking();
			l.setItem(item);
			l.setTransID(tId);
			l.setLockState(LOCK.WRITELOCK);
			LockingTable.put(item, l);
			}
			else
			{
				if(LockingTable.get(item).getLockState() == LOCK.UNLOCK)
				{
					LockingTable.get(item).setLockState(LOCK.WRITELOCK);
					LockingTable.get(item).setTransID(tId);
				}
				else if(LockingTable.get(item).getLockState() == LOCK.WRITELOCK)
				{
					String transactionID = LockingTable.get(item).getTransID();
					if( TransactionTable.get(tId).getTimeStamp() < TransactionTable.get(transactionID).getTimeStamp())
					{
						//Put tid on LinkedHashTable wala WaitTable 
						WaitQueue wq = new WaitQueue();
						wq.setItem(item);
						wq.setTransID(tId);
						wq.setItemLock(LOCK.WRITELOCK);
						WaitTable.put(item, wq);
						
						TransactionTable.get(tId).setState(STATE.BLOCKED);
					}
					else
					{
						fileReader.Unlock(tId);
						TransactionTable.get(tId).setState(STATE.ABORTED);
						LockingTable.get(item).setTransID(LockingTable.get(item).getTransID().substring(1));
					}
				}
				else//else(if the item id readlocked)
				{
					if(LockingTable.get(item).read_counter == 1 && Integer.parseInt(tId) == Integer.parseInt(LockingTable.get(item).getTransID()))
					{
						LockingTable.get(item).setLockState(LOCK.WRITELOCK);
						LockingTable.get(item).read_counter--;
					}
					else// multiple reads
					{
						String transactionID = LockingTable.get(item).getTransID();
						System.out.println("transactionID value = "+ transactionID );
						//break the transID string
						char [] transactionArray = transactionID.toCharArray();
						
						for(int i = 0; i < transactionArray.length ; i++)
						{
							System.out.println("i = "+i);
							String transArray = Character.toString(transactionArray[i]);
							System.out.println("transArray value = "+ transArray );
							System.out.println("tId value = "+ tId );
							System.out.println("transArray.timestamp value = "+ TransactionTable.get(transArray).getTimeStamp() );
							System.out.println("tId.timestamp value = "+ TransactionTable.get(tId).getTimeStamp() );
							
							if( TransactionTable.get(tId).getTimeStamp() < TransactionTable.get(transArray).getTimeStamp())
							{
								//Put tid on LinkedHashTable wala WaitTable
								WaitQueue wq = new WaitQueue();
								wq.setItem(item);
								wq.setTransID(tId);
								wq.setItemLock(LOCK.WRITELOCK);
								WaitTable.put(item, wq);
								
								TransactionTable.get(tId).setState(STATE.BLOCKED);
							}
							else if( TransactionTable.get(tId).getTimeStamp() == TransactionTable.get(transArray).getTimeStamp())
							{
								
							}
							else
							{
								fileReader.Unlock(tId);
								TransactionTable.get(tId).setState(STATE.ABORTED);
								//remove char from the string
								LockingTable.get(item).setTransID(LockingTable.get(item).getTransID().substring(1));
							}
						}
					}
				}
			}
		}
	}
	
	/*Unlock function is used to release all locks and wake up the next transaction from wait queue.*/
	public static void Unlock(String transID)
	{
		//while(TransactionTable.get(transID).getState() != STATE.BLOCKED)
		//{
			Set<String> keys = LockingTable.keySet();
			for(String key: keys)
			{
				if(LockingTable.get(key).getTransID().equals(transID))
				{
					if(LockingTable.get(key).getLockState() == LOCK.WRITELOCK)
					{
						LockingTable.get(key).setLockState(LOCK.UNLOCK);
						//extract the first transaction for that item and update the lock table
						
						if(!WaitTable.isEmpty())
						{
							Object val = WaitTable.entrySet().iterator().next();
							WaitQueue q = WaitTable.get(val);
							String t = q.getTransID();
							LOCK l = q.getItemLock();
							if(l == LOCK.WRITELOCK)
							{
								//LockingTable.get(key).read_counter++;
								WriteOperation(t, key);
							}
							LockingTable.get(key).setTransID(t);
							LockingTable.get(key).setLockState(l);
							WaitTable.remove(val);
							System.out.println("Value removed from the table: "+ val);
							
							//TransactionTable.get(t).setState(STATE.ACTIVE);//After waking up set the transaction's state as Active.						
						}					
					}
					else if(LockingTable.get(key).getLockState() == LOCK.READLOCK)
					{
						if(LockingTable.get(key).read_counter == 1)
						{
							LockingTable.get(key).setLockState(LOCK.UNLOCK);
							//extract the first transaction for that item and update the lock table
							
							if(!WaitTable.isEmpty())
							{
								Object val = WaitTable.entrySet().iterator().next();
								WaitQueue q = WaitTable.get(val);
								String t = q.getTransID();
								LOCK l = q.getItemLock();
								if(l == LOCK.READLOCK)
								{
									LockingTable.get(key).read_counter++;
									ReadOperation(t, key);
								}
								LockingTable.get(key).setTransID(t);
								LockingTable.get(key).setLockState(l);
								WaitTable.remove(val);
								System.out.println("Value removed from the table: "+ val);
								
								//TransactionTable.get(t).setState(STATE.ACTIVE);						
							}
						}
						LockingTable.get(key).read_counter --;
					}
				}
		    }
		//}
	}
	
	public static void display()
	{
		//Printing the output.
		System.out.println();
		 System.out.println("-------------------------------------------------");
		 System.out.println("|\t\tTRANSACTION TABLE\t\t|");
		 System.out.println("-------------------------------------------------");
		 System.out.println("-TRANSACTION_ID   |     TIMESTAMP     |    STATE-");
		 System.out.println("-------------------------------------------------");
		 //System.out.println("Value:"+ t1.getTransID()+ "  "+t1.getTimeStamp()+ "  "+t1.getState());
		 Set<String> keys = TransactionTable.keySet();
		 for(String key: keys)
		 {
			 Transaction t1 = TransactionTable.get(key);
			 
			 System.out.println("\t"+ t1.getTransID()+ "\t\t    "+t1.getTimeStamp()+ "  \t\t"+t1.getState());	
		 }
		 
		 System.out.println("\n\n---------------------------------------------");
		 System.out.println("\t\t   LOCK TABLE   \t\t");
		 System.out.println("---------------------------------------------");
		 System.out.println("-ITEM   |     TRANSACTION_ID     |    STATE-");
		 System.out.println("---------------------------------------------");
		 Set<String> i = LockingTable.keySet();
		 for(String key: i)
		 {
			 Locking l = LockingTable.get(key);
			 
			 System.out.println("    "+ l.getItem()+ "\t\t    "+l.getTransID()+ "    \t\t"+l.getLockState());	
		 }
		 System.out.println();
		 
		 System.out.println("\n\n---------------------------------------------");
		 System.out.println("\t\t   Wait TABLE   \t\t");
		 System.out.println("---------------------------------------------");
		 System.out.println("-ITEM   |     TRANSACTION_ID     |    STATE-");
		 System.out.println("---------------------------------------------");
		 Set<String> j = WaitTable.keySet();
		 for(String key: j)
		 {
			 WaitQueue wq = WaitTable.get(key);
			 
			 System.out.println("    "+ wq.getItem()+ "\t\t    "+wq.getTransID()+ "    \t\t"+wq.getItemLock());	
		 }
	}
	
	
	public static void main(String[] args)
	{
		 Scanner s = null;
		 try
		 {
			 //Reading the file input.
			 s = new Scanner(new BufferedReader(new FileReader("D:\\EclipseCodes\\testProjectDB2\\src\\testProjectDB2\\transactionList.txt")));
			 while(s.hasNext())// Do until the end of the file.
			 {
				  String str = s.next();
				  char myChar = str.charAt(0);
				  System.out.println(str);
				  String tId;
				  String item;
				 
				  try
					  {
						  switch(myChar)
						  {
						  	//Begin operation of transaction.
						  	case 'b':	tId = str.substring(1,str.indexOf(";"));
						  				System.out.println("Transaction " + tId +" begins\n");
						  				Transaction tr = new Transaction();
						  				tr.setTransID(tId);
						  			    TransactionTable.put(tId, tr);
						  			    
						  				break;
						  	
						  	//Read operation of transaction.			
						  	case 'r':	tId = str.substring(1, str.indexOf("("));
						  				item = str.substring(str.indexOf("(") + 1, str.indexOf(")"));
						  				System.out.println("Transaction " + tId + " is attempting a READLOCK on " + item+ "\n");
						  				
						  				ReadOperation(tId, item);
						  				display();
						  				
						  				break;
						  	
						  	//Write operation of transaction.			
						  	case 'w':	tId = str.substring(1, str.indexOf("("));
						  				item = str.substring(str.indexOf("(") + 1, str.indexOf(")"));
						  				System.out.println("Transaction " + tId + " is attempting a WRITELOCK on " + item+"\n");
						  				
						  				WriteOperation(tId, item);
						  				display();
						  				
						  				break;
						  			
						  	//End operation of transaction.
						  	case 'e':	tId = str.substring(1, str.indexOf(";"));
						  				if(TransactionTable.get(tId).getState() == STATE.ABORTED )
						  				{
						  					System.out.println("Transaction " + tId +" is ABORTED\n");
						  				}
						  				else if(TransactionTable.get(tId).getState() == STATE.BLOCKED )
						  				{
						  					System.out.println("Transaction " + tId +" is BLOCKED\n");
						  					
						  				}
						  				else
						  				{
							  				fileReader.Unlock(tId);
									  		TransactionTable.get(tId).setState(STATE.COMMITED);
									  		System.out.println("End of Transaction " + tId +". Committed successfully.\n");
//									  		while(TransactionTable.get(tId).getState() == STATE.BLOCKED)
//									  		{
//									  			Unlock(tId);
//									  		}
						  				}
								  		display();
						  				break;
						  }
						  while(!WaitTable.isEmpty())
						  {
							  System.out.println("Blocked Item contained");
							  Set<String> keys = WaitTable.keySet();
							  for(String key: keys)
							  {
								  System.out.println("Key: " + key);
								  String tid = WaitTable.get(key).getTransID();
								  if(TransactionTable.get(tid).getState() == STATE.BLOCKED)
								  {
									  String t = WaitTable.get(key).getTransID();
									  System.out.println("t : "+t);
									  LOCK l = WaitTable.get(key).getItemLock();
									  if(l == LOCK.WRITELOCK)
									  {
										  TransactionTable.get(t).setState(STATE.ACTIVE);
										  WriteOperation(t, key);
										  if(TransactionTable.get(t).getState() != STATE.ABORTED)
										  {
											  TransactionTable.get(t).setState(STATE.COMMITED);
											  //LockingTable.get(key).setLockState(LOCK.UNLOCK);
										  }
									  }
									  else if(l == LOCK.READLOCK)
									  {
										  //LockingTable.get(key).read_counter++;
										  TransactionTable.get(t).setState(STATE.ACTIVE);
										  ReadOperation(t, key);
										  if(TransactionTable.get(t).getState() != STATE.ABORTED)
										  {
											  TransactionTable.get(t).setState(STATE.COMMITED);
											  //LockingTable.get(key).setLockState(LOCK.UNLOCK);
										  }
									  }
									  WaitTable.remove(key);
									  System.out.println("Value removed from the table: "+ key);
									  //LockingTable.get(key).setLockState(LOCK.UNLOCK);
								  }
							  }
						  }
					  }
					  catch(Exception e)
					  {
						e.printStackTrace();   
					  }
				  }
			 display();
		 }
		catch(IOException e)
		{
			e.printStackTrace();
		}
		
	}
}
