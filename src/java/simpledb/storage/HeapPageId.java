package simpledb.storage;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    private Integer m_tableId;
    private Integer m_pageNo;
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        m_tableId = tableId;
        m_pageNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return m_tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        return m_pageNo;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        int pageCode = m_pageNo.hashCode();
        pageCode = 17 * pageCode + m_tableId.hashCode();
        return pageCode;

        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if(o == null)
            return false;
        if((o instanceof HeapPageId ))
        {
           HeapPageId a = (HeapPageId) o;
            if(m_tableId.equals(a.m_tableId)&&m_pageNo.equals(a.m_pageNo))
                return true;
        }
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
