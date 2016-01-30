package simpledb;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    //taken from constructor. now global variables
    //array specifying the number of and types of fields in this
    //TupleDesc. It must contain at least one entry.
    Type[] _typeAr;

    //array specifying the names of the fields
    String[] _fieldAr;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        _typeAr = typeAr;
        _fieldAr = fieldAr;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        _typeAr = typeAr;
        _fieldAr = new String[typeAr.length]; // Need an empty array at least
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return _typeAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        //return the text in the string at i
        return _fieldAr[i];

//        return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return _typeAr[i];
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException{
        if(name == null || _fieldAr == null){
            throw new NoSuchElementException();
        }
       for(int i = 0; i < _fieldAr.length; i++) {
           if (_fieldAr[i].matches(name)) {
               return i;
           }
       }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int total = 0;
        for(int i = 0; i < this.numFields(); i++)
        {
            total += this.getFieldType(i).getLen();
        }
        return total;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {

        // we need to create  anew tuple. lets first find the length
        int length = td1.numFields() + td2.numFields();
        int len1 = td1.numFields();
        int len2 = td2.numFields();

        // create new variables for the new
        Type[] typeAr = new Type[length];
        String[] fieldAr = new String[length];

        int ticker = 0;
        //build new tupledesc
        for(int i = 0; i < len1; i++){
            typeAr[ticker] = td1._typeAr[i];
            fieldAr[ticker] = td1._fieldAr[i];
            ticker++;
        }
        for(int j = 0; j < len2; j++){
            typeAr[ticker] = td2._typeAr[j];
            fieldAr[ticker] = td2._fieldAr[j];
            ticker++;
        }

        return new TupleDesc(typeAr,fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if(o == null){
            return false;
        }
        if(!(o instanceof TupleDesc)){
            return false;
        }
        else if(this.numFields() == ((TupleDesc) o).numFields() && this.getSize() == ((TupleDesc) o).getSize()){
            return true;
        }
        else
            return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String string = "";
        if (_fieldAr == null) {
            for (int i = 0; i < this.numFields(); i++) {
                string += this.getFieldType(i) + "[" + i + "]";
            }
        } else {
            for (int i = 0; i < this.numFields(); i++) {
                string += this.getFieldType(i) + "[" + i + "]" + "(" + this.getFieldName(i) + i + ")";
            }
        }
        return string;
    }
}
