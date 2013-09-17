package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private Field[] tuple;
    private TupleDesc description;
    private RecordId recordID = null;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */

    public Tuple(TupleDesc td) {
        if(td.numFields() < 1)
            throw new IllegalArgumentException(td.toString());

        this.description = td;
        this.tuple = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.description;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.recordID;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordID = rid;
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
        if(i >= this.tuple.length || i < 0)
            throw new IllegalArgumentException(String.valueOf(i));

        if (f.getType() == Type.STRING_TYPE)
            this.tuple[i] = new StringField(((StringField) f).getValue(), Type.STRING_LEN);
        else
            this.tuple[i] = new IntField(((IntField) f).getValue());
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if(i >= this.tuple.length || i < 0)
            throw new IllegalArgumentException(String.valueOf(i));

        return this.tuple[i];
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
        String string = "";
        int i;

        for (i = 0; i < this.description.numFields() - 1; i++) {
            Field readField= this.tuple[i];
            if (readField != null) {
                if (readField.getType() == Type.STRING_TYPE)
                    string = string + ((StringField) readField).getValue() + "\t";
                else
                    string = string + ((IntField) readField).getValue() + "\t";
            }
            else
                string = string + "null" + "\t";
        }

        Field readField= this.tuple[i];
        if (readField != null) {
            if (readField.getType() == Type.STRING_TYPE)
                string = string + ((StringField) readField).getValue() + "\n";
            else
                string = string + ((IntField) readField).getValue() + "\n";
        }
        else
            string = string + "null" + "\n";

        return string;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return Arrays.asList(this.tuple).iterator();
    }
}
