package simpledb;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private Map<PageId, Page> theBufferPool;
    private int maxNumPages;
    private LockManager manager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        if(numPages < 1)
            throw new IllegalArgumentException(String.valueOf(numPages));
        else{
            maxNumPages = numPages;
            this.theBufferPool = Collections.synchronizedMap(new LinkedHashMap<PageId, Page>(maxNumPages+1, 0.75F, true) {
                public boolean removeEldestEntry(Map.Entry oldest) {
                    //PROBLEM: This method is invoked by put AFTER inserting a new entry into the map.

                    boolean removeOldest = this.size() > maxNumPages;   //we inserted +1 more than max?

                    if (((Page) oldest.getValue()).isDirty() != null)  //if the oldest IS dirty...
                        //throw new DbException("buffer full");
                        removeOldest = false;                          //dont remove it.

                    else if (removeOldest) {                           //if the oldest is NOT dirty, and we must remove...
                        try {
                            flushPage((PageId) oldest.getKey());       //remove the oldest
                        } catch (IOException e) {
                            System.err.println(e);
                        }
                    }

                    return removeOldest;                              //tells the linkedlist whether to remove the oldest
                }
            });
            manager = new LockManager();
        }
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        if(perm == Permissions.READ_WRITE)
            this.manager.addWriteLock(tid, pid);
        else
            this.manager.addReadLock(tid,pid);

        Page readPage = theBufferPool.get(pid);
        if(readPage != null){
            theBufferPool.put(pid, readPage);
            return readPage;
        }
        else{
            Page newpage = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
            theBufferPool.put(pid, newpage);
            return newpage;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        //System.out.println("releasing page " + pid.pageNumber() + " by " + tid.getId());
        this.manager.removeReadLock(tid, pid);
        this.manager.removewriteLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return (this.manager.holdsReadLock(tid, pid) || this.manager.holdsWriteLock(tid, pid));
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {

        if(commit)
           flushPages(tid);
        else {
            Iterator<Page> theIterator = this.theBufferPool.values().iterator();
            while(theIterator.hasNext()) {
                Page page = theIterator.next();
                PageId pid = page.getId();

                if(page.isDirty() == tid)
                    this.theBufferPool.put(pid, page.getBeforeImage());
           }
        }

        Iterator<Page> theIterator = this.theBufferPool.values().iterator();
        while(theIterator.hasNext()) {
            Page page = theIterator.next();
            PageId pid = page.getId();
            if(manager.holdsReadLock(tid,pid) || manager.holdsWriteLock(tid, pid))
                releasePage(tid, page.getId());
        }
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, TransactionAbortedException {

        DbFile file = Database.getCatalog().getDbFile(tableId);
        try {
            ArrayList<Page> pagesList = file.insertTuple(tid, t);

            for(int i=0; i<pagesList.size(); i++) {
                Page page = pagesList.get(i);
                page.markDirty(true,tid);
                this.theBufferPool.put(page.getId(),page);
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {

        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDbFile(tableId);
        Page page = file.deleteTuple(tid, t);
        page.markDirty(true,tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        Iterator<Page> theIterator = this.theBufferPool.values().iterator();

        while(theIterator.hasNext()) {
            Page page = theIterator.next();
            PageId pid = page.getId();
            DbFile file = Database.getCatalog().getDbFile(pid.getTableId());

            if (page.isDirty() != null) {
                page.markDirty(false, page.isDirty());
                file.writePage(page);
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        this.theBufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
        Page page = this.theBufferPool.get(pid);

        page.markDirty(false, page.isDirty());
        file.writePage(page);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        Iterator<Page> theIterator = this.theBufferPool.values().iterator();

        while(theIterator.hasNext()) {
            Page page = theIterator.next();
            PageId pid = page.getId();

            if(page.isDirty() == tid)
                flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        //need to evict page whenever: get() or put()
    }

}
