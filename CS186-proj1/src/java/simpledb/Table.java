package simpledb;

/**
 * Created by brianwallace on 1/15/16.
 */
public class Table {

    private int _tableID;
    private DbFile  _file;
    private TupleDesc _tupleDesc;
    private String _name;
    private String _pkeyField;

    public Table(int _tableID, DbFile _file, String _name, String _pkeyField) {
        this._tableID = _tableID;
        this._file = _file;
        this._tupleDesc = _file.getTupleDesc();
        this._name = _name;
        this._pkeyField = _pkeyField;
    }

    public Table(int _tableID, DbFile _file, TupleDesc _tupleDesc, String _name) {
        this._tableID = _tableID;
        this._file = _file;
        this._tupleDesc = _tupleDesc;
        this._name = _name;
    }

    public int get_tableID() {
        return _tableID;
    }

    public void set_tableID(int _tableID) {
        this._tableID = _tableID;
    }

    public DbFile get_file() {
        return _file;
    }

    public void set_file(DbFile _file) {
        this._file = _file;
    }

    public TupleDesc get_tupleDesc() {
        return _tupleDesc;
    }

    public void set_tupleDesc(TupleDesc _tupleDesc) {
        this._tupleDesc = _tupleDesc;
    }

    public String get_name() {
        return _name;
    }

    public void set_name(String _name) {
        this._name = _name;
    }

    public String get_pkeyField() {
        return _pkeyField;
    }

    public void set_pkeyField(String _pkeyField) {
        this._pkeyField = _pkeyField;
    }
}
