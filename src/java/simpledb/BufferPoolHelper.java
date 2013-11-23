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
        //System.out.println(numPages);
        map = new ConcurrentHashMap<PageId, Page>();
        list = new ArrayList<PageId>();
    }

    public void put(PageId pid, Page page) throws DbException{
        boolean done = false;
        //System.out.println("list size: " + list.size());
        if (list.size() < maxPages)    {
            if (list.contains(pid)) {
                list.remove(pid);
                list.add(pid);
                //System.out.println("removed+add1: " + pid.toString());
                map.put(pid, page);
                done = true;
            }
            else {
                list.add(pid);
                //System.out.println("added1: " + pid.toString());
                map.put(pid, page);
                done = true;
            }
        }
        else {
            if (list.contains(pid)) {
                list.remove(pid);
                list.add(pid);
                //System.out.println("removed+add2: " + pid.toString());
                map.put(pid, page);
                done = true;
            }
            else {
            for (int i = 0; i < list.size(); i++) {
                PageId id = list.get(i);
                Page pg = map.get(id);
                //System.out.println("dirty: " + id.toString() + " " + pg.isDirty());

                //System.out.println("LIST: ");
                //for(int ii =0; ii<list.size(); ii++)
                //    System.out.println(list.get(ii));
                //System.out.println("END");


                if (pg.isDirty() == null) {
                    this.remove(id);
                    list.add(pid);
                    map.put(pid, page);
                    //System.out.println("added3: " + pid.toString());
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
            //System.out.println("removed+add: " + pid.toString());
            return page;
        }
        else
            return null;
    }

    public void remove(PageId pid) {
        boolean b = list.remove(pid);
        //System.out.println(b);
        map.remove(pid);
        //System.out.println("removed: " + pid.toString());
    }

    public Iterator<Page> iterator() {
         return map.values().iterator();
    }
}
