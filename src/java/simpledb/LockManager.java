package simpledb;


import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private ConcurrentHashMap<PageId, TransactionId> writeMap;
    private ConcurrentHashMap<PageId, ArrayList<TransactionId>> readMap;
    private static final int TIMEOUT = 100;

    public LockManager() {
        writeMap = new ConcurrentHashMap<PageId, TransactionId>();
        readMap = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
    }

    public synchronized void addReadLock(TransactionId tid, PageId pid) throws InterruptedException{

        ArrayList<TransactionId> transList = readMap.get(pid);

        if(transList == null) {
            transList = new ArrayList<TransactionId>();
            transList.add(tid);
            readMap.put(pid, transList);
            System.out.println("READ LOCK ADDED");
        }
        else if(!transList.contains(tid)) {
            transList.add(tid);
            readMap.put(pid, transList);
            System.out.println("REAd LOCK ADDED");
        }
        //else
            //Thread.sleep(TIMEOUT);

    }

    public synchronized void addWriteLock(TransactionId tid, PageId pid) throws InterruptedException{

        while(true) {
                //System.out.println("BAA");
                if(writeMap.get(pid) == null) {
                    writeMap.put(pid, tid);
                    System.out.println("WRITE LOCK ADDED");
                    break;
            }
            //else
                //Thread.sleep(TIMEOUT);
        }
    }

    public synchronized void removeReadLock(TransactionId tid, PageId pid) {
        ArrayList<TransactionId> transList = readMap.get(pid);
        readMap.remove(tid);
        readMap.put(pid, transList);
        System.out.println("READ LOCK REMOVED");
    }

    public void removewriteLock(TransactionId tid, PageId pid) {
        writeMap.remove(pid);
        System.out.println("WRITE LOCK REMOVED");
    }

    public boolean holdsReadLock(TransactionId tid, PageId pid) {
        return readMap.get(pid).contains(tid);
    }

    public boolean holdsWriteLock(TransactionId tid, PageId pid) {
        return writeMap.get(pid) == tid;
    }


}
