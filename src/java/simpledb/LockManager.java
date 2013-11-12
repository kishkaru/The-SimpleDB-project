package simpledb;


import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private ConcurrentHashMap<PageId, TransactionId> writeMap;
    private ConcurrentHashMap<PageId, ArrayList<TransactionId>> readMap;
    private static final int TIMEOUT = 10;

    public LockManager() {
        writeMap = new ConcurrentHashMap<PageId, TransactionId>();
        readMap = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
    }

    public synchronized void addReadLock(TransactionId tid, PageId pid) throws InterruptedException{
        if (writeMap.containsKey(pid) && writeMap.get(pid) == tid) {
            ArrayList<TransactionId> transList = readMap.get(pid);
            if(transList == null) {
                transList = new ArrayList<TransactionId>();
                transList.add(tid);
                readMap.put(pid, transList);
            }
            else if(!transList.contains(tid)) {
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

    public synchronized void addWriteLock(TransactionId tid, PageId pid) throws InterruptedException{
        if (readMap.containsKey(pid) && readMap.get(pid).contains(tid) && readMap.get(pid).size() == 1) {
            writeMap.put(pid, tid);
        }

        while(true) {
            //System.out.println(writeMap.get(pid));
            if(!writeMap.containsKey(pid)) {
                this.addReadLock(tid,pid);
                writeMap.put(pid, tid);
                //System.out.println("WRITE LOCK ADDED  BY: " + tid.getId() );
               break;
            }
            else
                Thread.sleep(TIMEOUT);
        }
    }

    public synchronized void removeReadLock(TransactionId tid, PageId pid) {
        ArrayList<TransactionId> transList = readMap.get(pid);
        transList.remove(tid);
        readMap.put(pid, transList);
        //System.out.println("READ LOCK REMOVED BY: " + tid.getId() );
    }

    public synchronized void removewriteLock(TransactionId tid, PageId pid) {
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
