package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    private PageId _pid;
    private int _tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        _pid = pid;
        _tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return _tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return _pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // must be a record ID
        if(!(o instanceof RecordId)){
            return false;
        }
        else if(this._tupleno == ((RecordId) o).tupleno() ){
            //if record ID and table ID are different, teh two records are different
            if(this.getPageId().getTableId() != ((RecordId) o).getPageId().getTableId()){
                return false;
            }
            return true;
        }
        else{
            return false;
        }
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        //needs to account for negative numbers
        int i = _pid.hashCode();
        String hashcode = "" + Math.abs(_tupleno) + Math.abs(i);
        Integer j = Integer.parseInt(hashcode);
        return j.intValue();

    }

}
