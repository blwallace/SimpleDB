package simpledb;

import org.apache.mina.util.ConcurrentHashSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by junyuanlau on 19/3/16.
 */
public class LockManager {
    private final ConcurrentHashMap<PageId, Object> locks =  new ConcurrentHashMap<PageId, Object>();;

    private ConcurrentHashMap<PageId,HashSet<TransactionId>> shared= new ConcurrentHashMap<PageId,HashSet<TransactionId>>();;
    private ConcurrentHashMap<PageId,TransactionId> exclusive = new ConcurrentHashMap<PageId,TransactionId>();;
    private ConcurrentHashMap<TransactionId,HashSet<TransactionId>> dependents = new ConcurrentHashMap<TransactionId,HashSet<TransactionId>>();;
    private ConcurrentHashSet<TransactionId> expiredTransactionSet = new ConcurrentHashSet<TransactionId>();
    private ConcurrentLinkedQueue<TransactionId> transactionQueue = new ConcurrentLinkedQueue<TransactionId>();

    private volatile ConcurrentHashMap<TransactionId, Long> allTransactions = new ConcurrentHashMap<TransactionId, Long>();
    private long start = 0;

    public void getLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        Object lock = getLock(pid);
        if (start == 0)
            start = System.currentTimeMillis();
        if (!transactionQueue.contains(tid)) {
                synchronized (this) {

                transactionQueue.add(tid);
                    /*
                try {
                    TimeUnit.MILLISECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                */
                long t0 = System.currentTimeMillis();
                allTransactions.put(tid, t0);
            }
            //System.out.println("put " + tid);
        }
        //System.out.println("time lock " + tid + " " + pid + " " + perm + allTransactions.get(tid));
        //System.out.println("" + transactionQueue.peek() + allTransactions.get(transactionQueue.peek()));
        //System.out.println(transactionQueue.peek() != tid && (System.currentTimeMillis() - allTransactions.get(transactionQueue.peek())) < 100);
        while (transactionQueue.peek() != tid &&
                ((System.currentTimeMillis() - (allTransactions.get(transactionQueue.peek())) <
                        ((allTransactions.size() < 15) ? Math.min(200, allTransactions.size()*50): 0))) && (System.currentTimeMillis() - start < 10000));
        //transactionQueue.poll();



        synchronized (lock) {

            for (TransactionId t : allTransactions.keySet()) {
                allTransactions.put(t,  System.currentTimeMillis());
                //allTransactions.put(tid, System.currentTimeMillis());
            }
            int timer = 0;
            while (isLocked(tid, pid, perm)) {
                if (timer > 25) {
                    if (shared.get(pid) != null && !expiredTransactionSet.contains(tid)) {
                        for (TransactionId id : shared.get(pid)) {
                            if (tid != id)
                                throw new TransactionAbortedException();

                        }
                        break;
                    }
                    if (exclusive.get(pid)!= null)
                        throw new TransactionAbortedException();

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
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        TransactionId tempTid;
        if(shared.containsKey(pid)){
            if (shared.get(pid).contains(tid))
                return true;
        }
        if(exclusive.containsKey(pid)){
            tempTid = exclusive.get(pid);
            if(tempTid == tid){
                return true;
            }
        }
        return false;
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
    public void releaseAllLocks(TransactionId tid ){
        expiredTransactionSet.add(tid);

        for (PageId pid : exclusive.keySet()){
            if (exclusive.get(pid) == tid)
                exclusive.remove(pid);
        }
        for (PageId pid : shared.keySet()){
            removeFromShared(pid, tid);
        }
        transactionQueue.remove(tid);

    }

    public void releaseLock(TransactionId tid, PageId pid) {
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
    private Object getLock(PageId pageId) {
        locks.putIfAbsent(pageId, new Object());
        return locks.get(pageId);
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
}
