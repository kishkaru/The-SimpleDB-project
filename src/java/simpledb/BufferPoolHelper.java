package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class BufferPoolHelper {

    private int maxPages;
    private ConcurrentHashMap<PageId, Page> map;
    private ArrayList<PageId> list;

    public BufferPoolHelper(int numPages) {
        maxPages = numPages;
        map = new ConcurrentHashMap<PageId, Page>();
        list = new ArrayList<PageId>();
    }

    public void put(PageId pid, Page page) throws DbException{
        boolean done = false;
        if (list.size() < maxPages)    {
            if (list.contains(pid)) {
                list.remove(pid);
                list.add(pid);
                map.put(pid, page);
                done = true;
            }
            else {
                list.add(pid);
                map.put(pid, page);
                done = true;
            }
        }
        else {
            if (list.contains(pid)) {
                list.remove(pid);
                list.add(pid);
                map.put(pid, page);
                done = true;
            }
            else {
                for (int i = 0; i < list.size(); i++) {
                    PageId id = list.get(i);
                    Page pg = map.get(id);
    
                    if (pg.isDirty() == null) {
                        this.remove(id);
                        list.add(pid);
                        map.put(pid, page);
                        done = true;
                        break;
                    }
                }
            }
        }

        if (!done) {
            throw new DbException("Could not add page to buffer pool");
        }
    }

    public Page get(PageId pid) {
        if(list.contains(pid)) {
            list.remove(pid);
            list.add(pid);
            Page page = map.get(pid);
            return page;
        }
        else
            return null;
    }

    public void remove(PageId pid) {
        boolean b = list.remove(pid);
        map.remove(pid);
    }

    public Iterator<Page> iterator() {
         return map.values().iterator();
    }
}
