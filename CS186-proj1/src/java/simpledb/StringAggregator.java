package simpledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private Op what;
    private boolean isOpen;
    private TupleDesc td;
    // create hashmaps to store aggregate values
    private HashMap<Field,Field> strFieldCount;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        this.what = what;
        this.strFieldCount = new HashMap<Field,Field>();
        this.isOpen = false;

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tupleGroup;

        // variables for our td
        Type[] typeAr;
        String[] fieldAr;

        if(!isOpen){
            isOpen = true;
            if(gbfield != Aggregator.NO_GROUPING){
                Type groupType = tup.getTupleDesc().getFieldType(gbfield);
                String groupField = tup.getTupleDesc().getFieldName(gbfield);
                typeAr = new Type[]{groupType, Type.INT_TYPE};
                fieldAr = new String[]{groupField, tup.getTupleDesc().getFieldName(afield)};
            }
            else{
                fieldAr = new String[]{tup.getTupleDesc().getFieldName(afield)};
                typeAr = new Type[]{Type.INT_TYPE};
            }
            td = new TupleDesc(typeAr,fieldAr);
        }

        // grouping logic
        if(gbfield == Aggregator.NO_GROUPING){
            tupleGroup = new IntField(Aggregator.NO_GROUPING);
        }
        else {
            tupleGroup = tup.getField(gbfield);
        }

        IntField tga = (IntField)tupleGroup;
        int tg = tga.getValue();
        IntField tv = (IntField) strFieldCount.get(tga);

        if(what != Op.COUNT){
            throw new IllegalArgumentException();
        }
        else{
                // if the value doesn't exist in the hashmap, put it in the hashmap
            if(strFieldCount.get(tupleGroup) == null){
                strFieldCount.put(tupleGroup,new IntField(1));
                }
                // else we add 1 to the existing tuple sum
            else{
                strFieldCount.put(tupleGroup,new IntField(tv.getValue() + 1));
                }
        }

    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        Iterator iterator = strFieldCount.entrySet().iterator();
        Tuple[] tupleIt = new Tuple[strFieldCount.size()];
        int i = 0;

        while(iterator.hasNext()){

            Map.Entry pair = (Map.Entry)iterator.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());
            Tuple tup = new Tuple(td);
            if(Aggregator.NO_GROUPING == gbfield){
                tup.setField(0, (Field) pair.getValue());
            }
            else{
                tup.setField(0, (Field) pair.getKey());
                tup.setField(1, (Field) pair.getValue());
            }
            tupleIt[i] = (tup);
            i++;
        }

        TupleIterator finalIt = new TupleIterator(td,Arrays.asList(tupleIt));
        return finalIt;
    }



}
