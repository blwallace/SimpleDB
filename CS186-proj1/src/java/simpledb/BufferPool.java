package simpledb;


import org.apache.mina.util.ConcurrentHashSet;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

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

    // private global variables. _numpages is dicated when object is created
    private int _numPages;
    private HashMap<PageId, Page> _bufferPool;
    private LinkedList<PageId> _linkedList;
    private final ConcurrentHashMap<PageId, Object> locks =  new ConcurrentHashMap<PageId, Object>();;

    private ConcurrentHashMap<PageId,HashSet<TransactionId>> shared;
    private ConcurrentHashMap<PageId,TransactionId> exclusive;
    private ConcurrentHashMap<TransactionId,HashSet<TransactionId>> dependents = new ConcurrentHashMap<TransactionId,HashSet<TransactionId>>();;
    private ConcurrentHashSet<TransactionId> expiredTransactionSet = new ConcurrentHashSet<TransactionId>();
    private ConcurrentLinkedQueue<TransactionId> transactionQueue = new ConcurrentLinkedQueue<TransactionId>();


    int timeOut = 0;

    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        _numPages = numPages;
        //create a new bufferpool on memory _numPages long
        _bufferPool = new HashMap<PageId, Page>();
        _linkedList = new LinkedList<PageId>();
        shared = new ConcurrentHashMap<PageId,HashSet<TransactionId>>();
        exclusive = new ConcurrentHashMap<PageId,TransactionId>();

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
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException, IOException {

        if(tid == null){
            tid = new TransactionId();
        }

        // page shared + write
        // page shared + read
        // page ex + read
        // page ex + write
//        if (expiredTransactionSet.contains(tid))
//            throw new TransactionAbortedException();

        Object lock = getLock(pid);
        if (!transactionQueue.contains(tid))
            transactionQueue.add(tid);


        synchronized (lock) {
            int timer = 0;
            while (isLocked(tid, pid, perm)) {
                if (timer > 60) {
                    if (shared.get(pid) != null && !expiredTransactionSet.contains(tid)) {
                        for (TransactionId id : shared.get(pid)) {
                            if (tid != id)
                                transactionComplete(id, false);
                        }
                        break;
                    }
                    if (exclusive.get(pid)!= null)
                        transactionComplete(exclusive.get(pid), false);

                    System.out.println("BREAK " + tid);
                }
                try {
                    timer++;
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            setLock(tid, pid, perm);

        }


        // look in bufferpool to see if page is present
        if(!_bufferPool.containsKey(pid)){
            // if page is not present but bufferpool is full
            if(_bufferPool.size() >= _numPages) {
                evictPage();
            }
            // add page to bufferpool
            _bufferPool.put(pid, Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid));
        } else {
            // remove the pid from the linked list
            _linkedList.remove(pid);
        }
        //add the pid to the top of the linked list again
        _linkedList.add(pid);

        //if (perm == Permissions.READ_ONLY)
        //    removeFromShared(pid, tid);
        return _bufferPool.get(pid);
    }

    private Object getLock(PageId pageId) {
        locks.putIfAbsent(pageId, new Object());
        return locks.get(pageId);
    }

    public boolean inShared(PageId pid, TransactionId tid) {
        HashSet<TransactionId> set = shared.get(pid);
        if (set == null)
            return false;
        return set.contains(tid);
    }

    public void addDependent(TransactionId primaryID, TransactionId dependent) {
        if (!dependents.containsKey(primaryID))
            dependents.put(primaryID, new HashSet<TransactionId>());
        HashSet<TransactionId> set = dependents.get(primaryID);
        set.add(dependent);
    }

    public boolean checkIfDependentsDone(TransactionId tid) {
        if (dependents.get(tid) == null)
            return true;
        for ( TransactionId id : dependents.get(tid)) {
            if (!expiredTransactionSet.contains(id))
                return false;
        }
        return true;
    }


    public boolean isLocked(TransactionId tid, PageId pid, Permissions perm){
        if (perm == Permissions.READ_WRITE) {
            return isSharedLocked(pid, tid) || isExclusiveLocked(pid, tid);
        } else if (perm == Permissions.READ_ONLY){
            return isExclusiveLocked(pid, tid);
        }
        return false;
    }


    public boolean isSharedLocked(PageId pid, TransactionId tid) {
        HashSet<TransactionId> set = shared.get(pid);
        if (set == null || set.contains(tid) || set.size() > 1)
            return false;
        return true;
    }

    public boolean isExclusiveLocked(PageId pid, TransactionId tid) {
        return exclusive.get(pid) != tid && exclusive.get(pid) != null;
    }

    public void removeFromShared(PageId pid, TransactionId tid){

        HashSet<TransactionId> set = shared.get(pid);
        if (set != null)
            set.remove(tid);
    }

    public void putToShared(PageId pid, TransactionId tid){
        HashSet<TransactionId> set = shared.get(pid);
        if (set == null) {
            set = new HashSet<TransactionId>();
        }
        set.add(tid);
        shared.put(pid, set);
    }
    //returns bufferpage size
    public int getPageSize(){
        return PAGE_SIZE;
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
        System.out.println("Manual Release" + tid + " " + pid);

        TransactionId tempTid;
        if(shared.containsKey(pid)){
            removeFromShared(pid, tid);
        }
        if(exclusive.containsKey(pid)){
            tempTid = exclusive.get(pid);
            if(tempTid == tid){
                exclusive.remove(pid);
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1

        transactionComplete(tid,true);
    }

    public void setLock(TransactionId tid, PageId pid, Permissions perm){
        //System.out.println("LOCK ME BABY " + tid.toString());
        if(perm == Permissions.READ_ONLY){
            putToShared(pid, tid);

            //shared.put(pid,tid);
        }
        if(perm == Permissions.READ_WRITE){
            exclusive.put(pid,tid);
        }

    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1

        TransactionId tempTid;
        if(shared.containsKey(pid)){
            if (inShared(pid, tid)){
                return true;
            }
        }
        if(exclusive.containsKey(pid)){
            tempTid = exclusive.get(pid);
            if(tempTid == tid){
                return true;
            }
        }
        return false;
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
        expiredTransactionSet.add(tid);

        // some code goes here
        // not necessary for proj1
        System.out.println("RELEASED " + tid.toString());


        Iterator it = _bufferPool.entrySet().iterator();

        if(commit){
            while(it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                HeapPage p = (HeapPage) pair.getValue();
                if (p.isDirty() != null && p.isDirty().equals(tid)) {
                    p.setBeforeImage();
                    flushPage(p.getId());
                }
            }
        } else{
            while(it.hasNext()) {
                Map.Entry pair = (Map.Entry)it.next();
                HeapPage p = (HeapPage) pair.getValue();
                if (p.isDirty() != null && p.isDirty().equals(tid)) {
                   _bufferPool.put(p.getId(),p.getBeforeImage());
                }
            }
        }
        for (PageId pid : exclusive.keySet()){
            if (exclusive.get(pid) == tid)
                exclusive.remove(pid);
        }
        for (PageId pid : shared.keySet()){
            if (inShared(pid, tid))
                removeFromShared(pid, tid);
        }
        transactionQueue.remove(tid);
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
            throws DbException, IOException, TransactionAbortedException {
        Database.getCatalog().getDbFile(tableId).insertTuple(tid,t);
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
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // iterate through all files and flush pages

        Iterator it = _bufferPool.entrySet().iterator(); // why entry set?

        //Iterator<PageId> it = _bufferPool.keySet().iterator(); // instead of this

        while (it.hasNext()) {

            Map.Entry pair = (Map.Entry)it.next();
            flushPage((PageId) pair.getKey());

            //flushPage(it.next()); // use this with keyset iterator
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for proj1
        _bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // we need to take a page out of the hashmap and print it to disk
        // get page from hashmap, file from db
        Page pg = _bufferPool.get(pid);
        DbFile file = Database.getCatalog().getDbFile(pid.getTableId());

        if(pg.isDirty() != null){
            //update the log
            Database.getLogFile().logWrite(pg.isDirty(),pg.getBeforeImage(),pg);
        }
        Database.getCatalog().getDbFile(pid.getTableId()).writePage(pg);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
        boolean cleanP = true;

        Iterator it = _bufferPool.entrySet().iterator();
        while(it.hasNext()) {
            cleanP = false;
            Map.Entry pair = (Map.Entry)it.next();
            HeapPage p = (HeapPage) pair.getValue();
            if (p.isDirty() == null) {
                it.remove();
                _linkedList.remove(p.getId());
            }
        }

        if(cleanP){
            throw new DbException("No mo clean pages");
        }
    }


}
