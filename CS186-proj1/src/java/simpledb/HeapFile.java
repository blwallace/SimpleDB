package simpledb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
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
        //load global variables
        _f = f;
        _td = td;

    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
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
    public int getId() {
        // some code goes here
        return _f.getAbsoluteFile().hashCode();
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
        //number of pages is the file size divided by the page size
        long pageSize = _f.length() / Database.getBufferPool().getPageSize();
        return (int) pageSize;

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

    /** Retrieve the number of tuples on this page.
     @return the number of tuples on this page
     */
    private int getNumTuples() {
        // some code goes here
        //floor((BufferPool.PAGE_SIZE*8) / (tuple size * 8 + 1))
        return (Database.getBufferPool().getPageSize() * 8) / (_td.getSize()*8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        return (int)Math.ceil(getNumTuples() / 8);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            //keeps track of the tuple
            int tickerTuple;
            //keeps track of the header
            int tickerHeader;
            // record keeping
            int numTuples = getNumTuples();
            int headSize = getHeaderSize();

            boolean open = false;


            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
                tickerHeader = 0;
                tickerTuple = (int) Math.ceil(getHeaderSize() / 8);

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(open == false){
                    return false;
                }
                int i = tickerHeader;
                while(i < numTuples){
                    if(i == 1){
                        return true;
                    }
                    i++;
                }
                return false;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(open == false || !hasNext()) {
                    throw new NoSuchElementException();
                }
                else{
                    // read fields in the tuple
                    Tuple t = new Tuple(_td);
                    try {
                        for (int j=0; j<_td.numFields(); j++) {
                            Field f = _td.getFieldType(j).parse(dis);
                            t.setField(j, f);
                        }
                    } catch (java.text.ParseException e) {
                        e.printStackTrace();
                        throw new NoSuchElementException("parsing error!");
                    }

                    return t;

                }
                return null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {

            }

            @Override
            public void close() {

            }


        };
    }



}

