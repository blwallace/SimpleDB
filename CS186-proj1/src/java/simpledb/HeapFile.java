package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

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

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc tupleDesc;

    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
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
        return Math.abs(file.getAbsoluteFile().hashCode()) % 10000000;

        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {


        byte[] bFile = new byte[(int) file.length()];
        FileInputStream fileInputStream;
        try
        {
            fileInputStream = new FileInputStream(file);
            fileInputStream.skip(pid.pageNumber() *  BufferPool.PAGE_SIZE);
            fileInputStream.read(bFile);
            //fileInputStream.read(bFile, pid.pageNumber() *  BufferPool.PAGE_SIZE, BufferPool.PAGE_SIZE);
            fileInputStream.close();

            return new HeapPage((HeapPageId) pid, bFile);
        }
        catch (Exception e)
        {
            //e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        /*
        if (page.isDirty() != null) {
            byte[] data = page.getPageData();
            FileOutputStream out = new FileOutputStream(file);
            out.write(data);
            out.close();
        }
        */
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(file.length()/ BufferPool.PAGE_SIZE);
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
        final TransactionId transactionId = tid;
        DbFileIterator it = new DbFileIterator() {

            private int pageNo = 0;
            private HeapPage page;
            private boolean isOpen = false;
            private Iterator<Tuple> currentIterator;
            @Override
            public void open() {
                isOpen = true;
                pageNo = 0;

                try {
                    page = (HeapPage) Database.getBufferPool().getPage(transactionId,new HeapPageId(getId(), pageNo),Permissions.READ_WRITE);
                    currentIterator = page.iterator();
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                } catch (DbException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!isOpen) {
                    return false;
                }
                if (page == null) {
                   return false;
                }

                if (currentIterator.hasNext()) {
                    return true;
                } else {
                    if (pageNo >= numPages())
                        return false;

                    while (true)
                    {
                        pageNo++;
                        if (pageNo>= numPages())
                            break;

                        try {
                            page = (HeapPage) Database.getBufferPool().getPage(transactionId, new HeapPageId(getId(), pageNo), Permissions.READ_WRITE);

                            if (page == null) {
                                return false;
                            } else {
                                currentIterator = page.iterator();

                                if (currentIterator.hasNext()) {
                                    return true;
                                }
                            }

                        } catch (TransactionAbortedException e) {
                            //e.printStackTrace();
                        } catch (DbException e) {
                            //e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    return false;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!isOpen) {
                    throw new NoSuchElementException();
                }

                if (page == null) {
                    throw new NoSuchElementException();
                }

                if (hasNext()) {
                    return currentIterator.next();
                } else {
                    throw new NoSuchElementException();
                }
            }


//                return currentIndex < fields.length && fields[currentIndex] != null;


            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!isOpen) {
                    throw new DbException("Iterator closed");
                }
                open();
            }

            @Override
            public void close() {
                isOpen = false;
            }
        };
        return it;

    }

}

