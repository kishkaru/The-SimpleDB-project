package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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
        return theList.iterator();
    }

    private static final long serialVersionUID = 1L;
    private ArrayList<TDItem> theList = new ArrayList<TDItem>();

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
         for(int i =0; i< typeAr.length; i++){
             TDItem someItem = new TDItem(typeAr[i], fieldAr[i]);
             theList.add(someItem);
         }
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
        for(int i =0; i< typeAr.length; i++){
            TDItem someItem = new TDItem(typeAr[i], null);
            theList.add(someItem);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return theList.size();
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
        if(i >= this.numFields() || i < 0)
            throw new NoSuchElementException();

        return theList.get(i).fieldName;
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
        if(i >= this.numFields() || i < 0)
            throw new NoSuchElementException();

        return theList.get(i).fieldType;
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
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        Iterator<TDItem> theIterator = this.iterator();
        int index = -1;
        boolean found = false;

        while(theIterator.hasNext()){
            index++;
            TDItem theItem = theIterator.next();
            String theName = theItem.fieldName;
            if(theName == null)
                continue;
            else if(theName.equals(name)){
                found = true;
                break;
            }
        }

        if(!found)
            throw new NoSuchElementException();

        return index;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;
        for(int i = 0; i<this.numFields(); i++){
            totalSize+= getFieldType(i).getLen();
        }

        return totalSize;
        //int oneTuple = Type.INT_TYPE.getLen() + Type.STRING_TYPE.getLen();
        //return oneTuple;
        //return oneTuple*this.numFields();
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
        Type[] theTypes = new Type[td1.numFields() + td2.numFields()];
        String[] theNames = new String[td1.numFields() + td2.numFields()];

        int i;
        for(i = 0; i< td1.numFields(); i++){
            theTypes[i] = td1.getFieldType(i);
            theNames[i] = td1.getFieldName(i);
        }

        for(int j = 0; j< td2.numFields(); j++){
            theTypes[i] = td2.getFieldType(j);
            theNames[i] = td2.getFieldName(j);
            i++;
        }

        return new TupleDesc(theTypes,theNames);
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
        boolean result = false;

        if(!(o instanceof TupleDesc))
            return result;
        else{
            TupleDesc td2 = (TupleDesc) o;
            if(td2.getSize() == this.getSize()){
                for(int i = 0; i< td2.numFields(); i++){
                    if(td2.getFieldType(i) == this.getFieldType(i))
                        result = true;
                    else{
                        result = false;
                        break;
                    }
                }
            }
        }

        return result;
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
        String description = "";

        int i;
        for(i = 0; i<this.numFields()-1; i++){
            description = description + this.getFieldType(i).toString() + "(" + this.getFieldName(i) + "),";
        }
        description = description + this.getFieldType(i).toString() + "(" + this.getFieldName(i) + ")";

        return description;
    }
}
