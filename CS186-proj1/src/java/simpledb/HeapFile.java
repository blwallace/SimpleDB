package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    //private variables for object
    File _f;
    TupleDesc _td;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        _f = f;
        _td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return _f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    //i had to get help on this part
    public int getId() {
        // some code goes here
        return Math.abs( _f.getAbsoluteFile().hashCode()) % 10000000;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return _td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        //creates an array of bytes
        byte[] bytes = getBinary();

        HeapPage _hp = null;
        try {
            _hp = new HeapPage((HeapPageId) pid,bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return _hp;
    }

    //converts the file to a byte array
    public byte[] getBinary(){
        byte[] byteArr = new byte[0];
        try {
            byteArr = Files.readAllBytes(_f.toPath());
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();}
        catch (IOException e) {
            e.printStackTrace();
        }
        return byteArr;
    }


    //used for testing purposes. prints out bit arrays
    public void printBinary(){
        //create byte array
        byte[] byteArr = new byte[0];

        try {
            byteArr = Files.readAllBytes(_f.toPath());
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();}
        catch (IOException e) {
            e.printStackTrace();
        }
        for (byte b : byteArr ) {
            System.out.println(Integer.toBinaryString(b & 255 | 256).substring(1));
        }

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(_f.length()/ BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        final TransactionId _tid = tid;
        DbFileIterator dbIterate = new DbFileIterator() {
            // stores iterator variables
            HeapPageId heapPageID;
            Iterator<Tuple> tupleIterator;
            //keeps track of the tuple count
            int tickerTuple = 0;
            //keeps track if our iterator is open
            boolean open = false;
            int pageTicker = 0;
            HeapPage heapPage;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                System.out.println("OPENED");

                open = true;
                pageTicker = 0;
                //hacky way to get an instance of our heappage.
                //we need this to extract tuples
                heapPageID = new HeapPageId(getId(),pageTicker);
                try {
                    heapPage = (HeapPage) Database.getBufferPool().getPage(_tid,heapPageID,Permissions.READ_ONLY);
                    tupleIterator =  heapPage.iterator();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                boolean foundPage = false;

                if(!open){
                    return false;
                }
                else if(heapPage == null){
                    return false;
                }

                if(tupleIterator.hasNext()){
                    return true;
                }
                // if current page is full, we need to iterate through the rest of the pages
                else{
                   while(foundPage){
                       pageTicker++;
                       try {
                           heapPage = (HeapPage) Database.getBufferPool().getPage(_tid,heapPageID,Permissions.READ_ONLY);
                           tupleIterator = heapPage.iterator();
                           if(tupleIterator.hasNext()){
                               return true;
                           }
                       } catch (IOException e) {
                           e.printStackTrace();
                       }
                   }
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!open){
                   throw new NoSuchElementException();
                }
                else if(heapPage == null){
                    throw new NoSuchElementException();
                }

                if(tupleIterator.hasNext()){
                    return tupleIterator.next();
                }
                else{
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();

            }

            @Override
            public void close() {
                open = false;
                pageTicker = 0;
            }


        };
                return dbIterate;
    }

}

