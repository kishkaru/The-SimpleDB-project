package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.sql.Timestamp;
import java.util.Date;

public class LockManager {
    private ConcurrentHashMap<PageId, TransactionId> writeMap;
    private ConcurrentHashMap<PageId, ArrayList<TransactionId>> readMap;
    private ConcurrentHashMap<TransactionId, ArrayList<PageId>> locks;
    private ConcurrentHashMap<TransactionId, Timestamp> startTimes;
    private static final int TIMEOUT = 100;
    private static final long TRANSACTION_TIME = 100;

    public LockManager() {
        writeMap = new ConcurrentHashMap<PageId, TransactionId>();
        readMap = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
	locks = new ConcurrentHashMap<TransactionId, ArrayList<PageId>>();
	startTimes = new ConcurrentHashMap<TransactionId, Timestamp>();
    }

    private void checkDeadlock(TransactionId tid) throws TransactionAbortedException {
	Date date = new Date();
	Timestamp now = new Timestamp(date.getTime());
	if (startTimes.containsKey(tid)) {
	    Timestamp start = startTimes.get(tid);
	    long startTime= start.getTime();
	    long currentTime = now.getTime();
	    if (currentTime - startTime > TRANSACTION_TIME) {
		if (locks.containsKey(tid)) {
		    ArrayList<PageId> pages = locks.get(tid);
		    for (PageId pid : pages) {
			Database.getBufferPool().releasePage(tid, pid);
		    }
		}
		throw new TransactionAbortedException(); 
	    }
	} else {
	    startTimes.put(tid, now);
	}
    }

    public synchronized void addReadLock(TransactionId tid, PageId pid) throws InterruptedException, TransactionAbortedException {
	checkDeadlock(tid); 

	// adding pid to locks, unsure about placement
	ArrayList<PageId> pages = new ArrayList<PageId>();
        if (locks.containsKey(tid)) 
	    pages = locks.get(tid);
	pages.add(pid);
	locks.put(tid, pages);       

	if (writeMap.containsKey(pid) && writeMap.get(pid) == tid) {
            ArrayList<TransactionId> transList = readMap.get(pid);
            if(transList == null) {
                transList = new ArrayList<TransactionId>();
                transList.add(tid);
                readMap.put(pid, transList);
            } else if(!transList.contains(tid)) {
                transList.add(tid);
                readMap.put(pid, transList);
            }
            return;
        }

        while(true) {
            if(!writeMap.containsKey(pid)) {
                ArrayList<TransactionId> transList = readMap.get(pid);
                if(transList == null) {
                    transList = new ArrayList<TransactionId>();
                    transList.add(tid);
                    readMap.put(pid, transList);
                    //System.out.println("READ LOCK ADDED BY: " + tid.getId() );
                     break;
                }
                else if(!transList.contains(tid)) {
                    transList.add(tid);
                    readMap.put(pid, transList);
                    //System.out.println("READ LOCK ADDED BY: " + tid.getId() );
                    break;
                }
            }

            Thread.sleep(TIMEOUT);
        }

    }

    public synchronized void addWriteLock(TransactionId tid, PageId pid) throws InterruptedException, TransactionAbortedException {
	checkDeadlock(tid); 

	// adding pid to locks, unsure about placement
	ArrayList<PageId> pages = new ArrayList<PageId>();
        if (locks.containsKey(tid)) 
	    pages = locks.get(tid);
	pages.add(pid);
	locks.put(tid, pages); 

        if (readMap.containsKey(pid) && readMap.get(pid).contains(tid) && readMap.get(pid).size() == 1) {
            writeMap.put(pid, tid);
        }
        if(writeMap.get(pid) == tid)
            return;

        while(true) {
            ArrayList<TransactionId> transList = readMap.get(pid);
            if(!writeMap.containsKey(pid) && (transList == null || transList.size() == 0)) {
                this.addReadLock(tid,pid);
                writeMap.put(pid, tid);
                break;
            }
            else {
                Thread.sleep(TIMEOUT);
            }
        }
    }

    public synchronized void removeReadLock(TransactionId tid, PageId pid) {
	if (locks.containsKey(tid)) {
	    ArrayList<PageId> pages = locks.get(tid);
	    pages.remove(pid);
	}

        ArrayList<TransactionId> transList = readMap.get(pid);
        transList.remove(tid);
        readMap.put(pid, transList);
        //System.out.println("READ LOCK REMOVED BY: " + tid.getId() );
    }

    public synchronized void removewriteLock(TransactionId tid, PageId pid) {
	if (locks.containsKey(tid)) {
	    ArrayList<PageId> pages = locks.get(tid);
	    pages.remove(pid);
	}

        writeMap.remove(pid);
        //System.out.println("WRITE LOCK REMOVED BY: " + tid.getId() );
    }

    public boolean holdsReadLock(TransactionId tid, PageId pid) {
        return readMap.get(pid).contains(tid);
    }

    public boolean holdsWriteLock(TransactionId tid, PageId pid) {
        return writeMap.get(pid) == tid;
    }


}
