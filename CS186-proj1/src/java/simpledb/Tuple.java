package simpledb;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {


    //This section creates private instances. we need to create a record id, tupleDesc,
    private RecordId _recordId;
    private TupleDesc _tupleDesc;
    private Field[] _field;



    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if(td == null){
            throw new NullPointerException();
        }

        // a tupledesc returns

        _field = new Field[td.numFields()];
        _tupleDesc = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        //bw return a Tuple Description
        return _tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return _recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        _recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        _field[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return _field[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        //create variable to build string
        String string = "";
        for(int i = 0; i < _field.length;i++){
            string += _field[i] + "\t";
        }
        string += "\n";
        System.out.printf(string);
        return string;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        Iterator<Field> it = new Iterator<Field>() {

            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < _field.length && _field[currentIndex] != null;
            }

            @Override
            public Field next() {
                return _field[currentIndex++];
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return it;
    }
}
