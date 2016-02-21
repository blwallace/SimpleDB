package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
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
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        Type fieldType;

        /**
         * The name of the field
         */
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
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        ArrayList<TDItem> list = new ArrayList<TDItem>();
        for (int i = 0; i < _typeAr.length; i++) {
            list.add(new TDItem(_typeAr[i], _fieldAr[i]));
        }

        return list.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        _typeAr = typeAr;
        _fieldAr = fieldAr;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
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
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        //return the text in the string at i
        return _fieldAr[i];

//        return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return _typeAr[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null || _fieldAr[0] == null) {
            throw new NoSuchElementException();
        }

        for (int i = 0; i < _fieldAr.length; i++) {
            String fieldName = _fieldAr[i];
            if (name.split("[.]").length > 1 && fieldName.split("[.]").length > 1) {
                if (fieldName.equals(name))
                    return i;

            } else if (name.split("[.]").length > 1){
                if (fieldName.equals(name.split("[.]")[1]))
                    return i;
            } else if (fieldName.split("[.]").length > 1){
                if (fieldName.split("[.]")[1].equals(name))
                    return i;
            } else {
                if (fieldName.equals(name))
                    return i;
            }

        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int total = 0;
        for (int i = 0; i < this.numFields(); i++) {
            total += this.getFieldType(i).getLen();
        }
        return total;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
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
        for (int i = 0; i < len1; i++) {
            typeAr[ticker] = td1._typeAr[i];
            fieldAr[ticker] = td1._fieldAr[i];
            ticker++;
        }
        for (int j = 0; j < len2; j++) {
            typeAr[ticker] = td2._typeAr[j];
            fieldAr[ticker] = td2._fieldAr[j];
            ticker++;
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        } else if (this.numFields() == ((TupleDesc) o).numFields() && this.getSize() == ((TupleDesc) o).getSize()) {
            return true;
        } else
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
        String result = "";
        for (int i = 0; i < _fieldAr.length; i++) {
            result += _typeAr[i].name();
            result += "(";
            result += _fieldAr[i];
            result += ")";
            if (i < _fieldAr.length - 1)
                result += ",";
        }
        return result;
        /*
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
        */
    }
    /*
    public void setTableName(String tableName){
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
    */

    public static String[] getAliasArray(DbIterator it) {
        if (it instanceof SeqScan) {
            String alias = ((SeqScan) it).getAlias();
            String[] result = new String[it.getTupleDesc().numFields()];
            for (int i = 0; i < result.length; i++) {
                result[i] = alias;
            }
            return result;
        } else if (it instanceof Filter) {
            return getAliasArray(((Filter) it).getChildren()[0]);
        } else if (it instanceof Project) {
            return getAliasArray(((Project) it).getChildren()[0]);
        } else if (it instanceof Join) {
            Join joinIt = (Join) it;
            String[] array1 = getAliasArray(joinIt.getChildren()[0]);
            String[] array2 = getAliasArray(joinIt.getChildren()[1]);
            String[] result = new String[it.getTupleDesc().numFields()];

            for (int i = 0; i < array1.length; i++) {
                result[i] = array1[i];
            }
            for (int i = 0; i < array2.length; i++) {
                result[i + array1.length] = array2[i];
            }
            return result;
        }
        return null;
    }

    public static TupleDesc createTupleDescWithAliasArray(TupleDesc tupleDesc, String[] aliases) {

        String[] newNames = new String[tupleDesc.numFields()];
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            if (tupleDesc.getFieldName(i) == null) {
                newNames[i] = null;

            } else {
                if (tupleDesc.getFieldName(i).split("[.]").length == 1) {
                    newNames[i] = aliases[i] + "." + tupleDesc.getFieldName(i);
                } else {
                    newNames[i] = aliases[i] + "." + tupleDesc.getFieldName(i).split("[.]")[1];
                }
            }
        }
        return new TupleDesc(tupleDesc._typeAr, newNames);
    }
}
