package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    private PageId page;
    private int slot;

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
        this.page = pid;
        this.slot = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return this.slot;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return this.page;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        boolean equals = false;
        if (!(o instanceof RecordId))
            equals = false;
        if( (((RecordId) o).getPageId().hashCode() == this.page.hashCode()) && (((RecordId) o).tupleno() == this.slot)) {
            equals = true;
        }

        return equals;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        String page = Integer.toString(this.page.hashCode());
        String slot = Integer.toString(this.slot);
        String hashCode = page + slot;
        return Integer.parseInt(hashCode);

    }

}
