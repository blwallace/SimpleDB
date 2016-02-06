package simpledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private int avgTicker;
    private int avgSum;
    private boolean isOpen;
    private TupleDesc td;

    // create hashmaps to store aggregate values
    private HashMap<Field,Field> intFieldCount;
    // This will be used for the average calculations
    private HashMap<Field,Field> intAvgCount;
    private HashMap<Field,Field> intSumCount;
    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.intFieldCount = new HashMap<Field,Field>();
        this.intAvgCount = new HashMap<Field,Field>();
        this.intSumCount = new HashMap<Field,Field>();
        this.avgTicker = 0;
        this.avgSum = 0;
        this.isOpen = false;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {

        Field tupleGroup;
        IntField tupleValue;

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
        tupleValue = (IntField)tup.getField(afield);

        IntField tva = (IntField)tupleValue;

        int tv = tva.getValue();

        //value of tuplegroup
        IntField tupGValue = (IntField) intFieldCount.get(tupleGroup);


        switch(what){
            case MIN:
                // if the value doesn't exist in the hashmap, put it in the hashmap
                if(intFieldCount.get(tupleGroup) == null){
                    intFieldCount.put(tupleGroup, new IntField(tupleValue.getValue()));
                }
                // if the current tuple is less than the current min, update the hashmap
                else if(tupGValue.getValue() > tv){
                        intFieldCount.put(tupleGroup,new IntField(tupleValue.getValue()));
                }
                break;
            case MAX:
                // if the value doesn't exist in the hashmap, put it in the hashmap
                if(intFieldCount.get(tupleGroup) == null){
                    intFieldCount.put(tupleGroup,new IntField(tupleValue.getValue()));
                }
                // if the current tuple is greater than the current min, update the hashmap
                else if(tupGValue.getValue() < tv){
                    intFieldCount.put(tupleGroup,new IntField(tupleValue.getValue()));
                }
                break;
            case SUM:
                // if the value doesn't exist in the hashmap, put it in the hashmap
                if(intFieldCount.get(tupleGroup) == null){
                    avgSum = tupleValue.getValue();
                    intFieldCount.put(tupleGroup,new IntField(avgSum));
                }
                // else we add our tupleValue to the existing tuple sum
                else{
                    intFieldCount.put(tupleGroup,new IntField(tupGValue.getValue() + tv));
                }
                break;
            case AVG:
                // if the value doesn't exist in the hashmap, put it in the hashmap
                if(intFieldCount.get(tupleGroup) == null){
                    avgSum = tupleValue.getValue();
                    intFieldCount.put(tupleGroup,new IntField(avgSum));
                    intAvgCount.put(tupleGroup,new IntField(1));
                    intSumCount.put(tupleGroup,new IntField(avgSum));
                }
                // else we add our tupleValue to the existing tuple sum
                else{
                    // we need to make calculations to find a enw average and maintain our hashtables
                    IntField count = (IntField)intAvgCount.get(tupleGroup);
                    IntField sum = (IntField)intSumCount.get(tupleGroup);

                    int newCount = count.getValue() + 1;
                    int newSum = sum.getValue() + tupleValue.getValue();
                    int avg = newSum / newCount;

                    intFieldCount.put(tupleGroup,new IntField(avg));
                    intAvgCount.put(tupleGroup,new IntField(newCount));
                    intSumCount.put(tupleGroup,new IntField(newSum));

                }
                break;
            case COUNT:
                // if the value doesn't exist in the hashmap, put it in the hashmap
                if(intFieldCount.get(tupleGroup) == null){
                    intFieldCount.put(tupleGroup,new IntField(1));
                }
                // else we add 1 to the existing tuple sum
                else{
                    intFieldCount.put(tupleGroup,new IntField(tupGValue.getValue() + 1));
                }
                break;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     *         http://stackoverflow.com/questions/1066589/iterate-through-a-hashmap
     */
    public DbIterator iterator() {

        // some code goes here
        Iterator iterator = intFieldCount.entrySet().iterator();
        Tuple[] tupleIt = new Tuple[intFieldCount.size()];
        int i = 0;

        while(iterator.hasNext()){
            Map.Entry pair = (Map.Entry)iterator.next();
//            System.out.println(pair.getKey() + " = " + pair.getValue());
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

    TupleIterator finalIt = new TupleIterator(td, Arrays.asList(tupleIt));
    return finalIt;
        //create a tuple iterator

    };


}
