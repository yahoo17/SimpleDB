package simpledb.storage;

import simpledb.common.Debug;
import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable ,Cloneable{

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public  List<TDItem> m_tuple_list = new ArrayList<TDItem>();

    @Override
    public TupleDesc clone() throws CloneNotSupportedException {
        TupleDesc tupleDesc = (TupleDesc) super.clone();
        tupleDesc.m_tuple_list = new ArrayList<>();
        for(Iterator<TDItem> iterator = this.m_tuple_list.iterator(); iterator.hasNext();)
        {
            TDItem old = iterator.next();
            tupleDesc.m_tuple_list.add(new TDItem(old.fieldType,old.fieldName));
        }
        return tupleDesc;
    }

    private long INT_COUNT = 0;
    private long STRING_COUNT =0;
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

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
        return m_tuple_list.iterator();
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
        for(int i = 0; i < typeAr.length; i++)
        {
            m_tuple_list.add(new TDItem(typeAr[i], fieldAr[i]));

        }
        // some code goes here
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
        for(int i = 0; i < typeAr.length; i++)
        {
            m_tuple_list.add(new TDItem(typeAr[i],null));
        }
        // some code goes here
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return m_tuple_list.size();
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
        return m_tuple_list.get(i).fieldName;
        // some code goes here
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
        // some code goes here
        return m_tuple_list.get(i).fieldType;
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
        // some code goes here
        if(name == null)
            throw new NoSuchElementException();
        for(int i = 0; i < m_tuple_list.size(); i++)
        {
            if(m_tuple_list.get(i).fieldName == null)
                continue;
            if(m_tuple_list.get(i).fieldName.equals(name))
            {
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
        // some code goes here
        int i_count = 0, s_count = 0;
        for(int i = 0; i < m_tuple_list.size(); i++)
        {
            Type t = m_tuple_list.get(i).fieldType;
            if(t==Type.INT_TYPE)
            {
                i_count++;
            }
            else
                s_count++;
        }
        return i_count*4 + s_count * Type.STRING_TYPE.getLen();
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
        // some code goes here
        int s1 = td1.numFields();
        int size = td1.numFields() + td2.numFields();
        Type [] mergeType = new Type[size];
        String[] mergeName = new String[size];
        for(int i = 0; i < td1.numFields(); i++)
        {
            mergeType[i] = td1.m_tuple_list.get(i).fieldType;
            mergeName[i] = td1.m_tuple_list.get(i).fieldName;
        }
        for(int i = td1.numFields();i < size ; i++)
        {
            mergeType[i] = td2.m_tuple_list.get(i-s1).fieldType;
            mergeName[i] = td2.m_tuple_list.get(i-s1).fieldName;
        }

        TupleDesc tupleDesc = new TupleDesc(mergeType,mergeName);
        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        //Debug.log("**111****");
        if( !( o instanceof  TupleDesc))
        {
            return  false;
        }
        //Debug.log("**2***");
        TupleDesc a = (TupleDesc) o;
        if(m_tuple_list.size() != a.numFields())
            return false;
        //Debug.log("**3***");
        for(int i = 0; i < m_tuple_list.size(); i++) {
            if (m_tuple_list.get(i).fieldType != a.m_tuple_list.get(i).fieldType||
                    m_tuple_list.get(i).fieldName != a.m_tuple_list.get(i).fieldName )
                return false;
        }
        //Debug.log("**4***");
        return true;
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
        // some code goes here
        String ans = "";
        for(int i = 0; i < m_tuple_list.size(); i++)
        {
            ans += m_tuple_list.get(i).fieldType.toString() + '(' + m_tuple_list.get(i).fieldName + ')';
        }
        return ans;
    }
}
