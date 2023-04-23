package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.text.html.HTMLDocument.Iterator;

import java.util.*;

/* revised slightly by sharma on 8/22/2022 */

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager reads disk pages into a mains memory page as needed. The
 * collection of main memory pages (called frames) used by the buffer manager
 * for this purpose is called the buffer pool. This is just an array of Page
 * objects. The buffer manager is used by access methods, heap files, and
 * relational operators to read, write, allocate, and de-allocate pages.
 * policy class name has to be changed in the constructior using name of the 
 * class you have implementaed
 */
public class BufMgr implements GlobalConst {

    /** Actual pool of pages (can be viewed as an array of byte arrays). */
    protected Page[] bufpool;

    /** Array of descriptors, each containing the pin count, dirty status, etc\
	. */
    protected FrameDesc[] frametab;

    /** Maps current page numbers to frames; used for efficient lookups. */
    protected HashMap<Integer, FrameDesc> pagemap;

    /** The replacement policy to use. */
    protected Replacer replacer;
    
//-------------------------------------------------------------
    /** 
        you may add HERE variables NEEDED for calculating hit ratios 
        a public void printBhrAndRefCount() has been provided at the bottom 
        which is called from test modules. To use that
            either use the same variable names OR
            modify the print method with variables you have used
    */     
//-------------------------------------------------------------

//Defining ArrayLists to keep track of page hits and requests
ArrayList<Integer> pid = new ArrayList<Integer>();
ArrayList<Integer> hit = new ArrayList<Integer>();
ArrayList<Integer> tot = new ArrayList<Integer>();
//Defining an ArrayList to include all the previous ArrayLists for Calculation
ArrayList<ArrayList<Integer>> array = new ArrayList<ArrayList<Integer>>();


    float totPageHits =0, totPageRequests=0;
    float aggregateBHR;
    int pageFaults=0;


  /**
   * Constructs a buffer mamanger with the given settings.
   * 
   * @param numbufs number of buffers in the buffer pool
   */
  public BufMgr(int numbufs) 
  {   
    //Adding all the arraylists into an arraylist of arraylists
    array.add(pid);
    array.add(hit);
    array.add(tot);
      numbufs = NUMBUF;
      System.out.println("The replacement policy is FIFO and the Buffer Size is " +numbufs);
	  //initializing buffer pool and frame table 
	  bufpool = new Page[numbufs];
      frametab = new FrameDesc[numbufs];
      
      
      for(int i = 0; i < frametab.length; i++)
      {
              bufpool[i] = new Page();
    	  	  frametab[i] = new FrameDesc(i);
      }
      
      //initializing page map and replacer here. 
      pagemap = new HashMap<Integer, FrameDesc>(numbufs);
      replacer = new FIFO(this);   // change Policy to replacement class name

    
  }

  /**
   * Allocates a set of new pages, and pins the first one in an appropriate
   * frame in the buffer pool.
   * 
   * @param firstpg holds the contents of the first page
   * @param run_size number of pages to allocate
   * @return page id of the first new page
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */

  public PageId newPage(Page firstpg, int run_size)
  {
	  //Allocating set of new pages on disk using run size.
	  PageId firstpgid = Minibase.DiskManager.allocate_page(run_size);
	  try {
		  //pin the first page using pinpage() function using the id of firstpage, page firstpg and skipread = PIN_MEMCPY(true)
		  pinPage(firstpgid, firstpg, PIN_MEMCPY);
        //   System.out.println("This is Run Size"+run_size);
          }
          catch (Exception e) {
        	  //pinning failed so deallocating the pages from disk
        	  for(int i=0; i < run_size; i++)
        	  {   
        		  firstpgid.pid += i;
 	  	          Minibase.DiskManager.deallocate_page(firstpgid);
        	  }

        	  return null;
      }
	  
	  //notifying replacer
      replacer.newPage(pagemap.get(Integer.valueOf(firstpgid.pid)));
      
      // you may have to add some BHR code here
      
      //return the page id of the first page
      return firstpgid; 
  }
  
  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) 
  {  
	  //the frame descriptor as the page is in the buffer pool 
	  FrameDesc tempfd = pagemap.get(Integer.valueOf(pageno.pid));
	  //the page is in the pool so it cannot be null.
      if(tempfd != null)
      {
    	  //checking the pin count of frame descriptor
          if(tempfd.pincnt > 0)
              throw new IllegalArgumentException("Page currently pinned");
          //remove page as it's pin count is 0, remove the page, updating its pin count and dirty status, the policy and notifying replacer.
          pagemap.remove(Integer.valueOf(pageno.pid));
          tempfd.pageno.pid = INVALID_PAGEID;
          tempfd.pincnt = 0;
          tempfd.dirty = false;
          tempfd.state = Policy.AVAILABLE;
          replacer.freePage(tempfd);
      }
      //deallocate the page from disk 
      Minibase.DiskManager.deallocate_page(pageno);
  }


  //Defining a function to increement the values in the ArrayList
  public static void increment(List<Integer> ar, int i) 
  {
    ar.set(i, ar.get(i) + 1);
}

  /**
   * Pins a disk page into the buffer pool. If the page is already pinned, this
   * simply increments the pin count. Otherwise, this selects another page in
   * the pool to replace, flushing it to disk if dirty.
   * 
   * @param pageno identifies the page to pin
   * @param page holds contents of the page, either an input or output param
   * @param skipRead PIN_MEMCPY (replace in pool); PIN_DISKIO (read the page in)
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public void pinPage(PageId pageno, Page page, boolean skipRead) 
  {  
    int ix,a,g,r_cnt=0;
	  //the frame descriptor as the page is in the buffer pool 
	  FrameDesc tempfd = pagemap.get(Integer.valueOf(pageno.pid));
	  if(tempfd != null)
	  {
		  //if the page is in the pool and already pinned then by using PIN_MEMCPY(true) throws an exception "Page pinned PIN_MEMCPY not allowed" 
          if(skipRead)
        	  throw new IllegalArgumentException("Page pinned so PIN_MEMCPY not allowed");
          else
          {
        	  //else the page is in the pool and has not been pinned so incrementing the pincount and setting Policy status to pinned

        	  tempfd.pincnt++;
        	  tempfd.state = Policy.PINNED;
              page.setPage(bufpool[tempfd.index]);
                
              //If the requested page is not in our arraylist, we add the page id to the arraylist and
              //initialize page hits to 1 and page loads to 1
              if((array.get(0).indexOf(pageno.pid)==-1)&&(pageno.pid>8))
              {
                array.get(0).add(pageno.pid);
                // a=array.get(0).indexOf(pageno.pid);
                array.get(1).add(1);
                array.get(2).add(1);
                
              }
              //If the requested page is already in the arraylist, we simply increment the hit count
              else if(pageno.pid>8)
              {
                ix = array.get(0).indexOf(pageno.pid);
                increment(array.get(1), ix);
                // increment(array.get(2), ix);
                //array.get(1).set(index, array.get(1).get(index) + 1);
              }
              //some BHR code may go here
            //   pageLoadHits = pageLoadHits + 1.0f; 
            
              return;
          }
	  }
	  else
	  {
		  //as the page is not in pool choosing a victim
          int i = replacer.pickVictim();
          //if buffer pool is full throws an Exception("Buffer pool exceeded")
          if(i < 0)
        	  throw new IllegalStateException("Buffer pool exceeded");
                
          tempfd = frametab[i];
          if(tempfd.pageno.pid!=-1)
          {
          for(g=0;g<array.get(2).size();g++)
                  {
                    if(array.get(0).get(g)==tempfd.pageno.pid)
                    r_cnt = array.get(1).get(g)+array.get(2).get(g);
                    break;
                  }
          if(r_cnt>0)        
          System.out.println("The victim page with Pid: "+tempfd.pageno.pid+ " has been referenced " +r_cnt + " times.");
          }
          //if the victim is dirty writing it to disk 
          if(tempfd.pageno.pid != -1)
          {
        	  pagemap.remove(Integer.valueOf(tempfd.pageno.pid));
        	  if(tempfd.dirty)
           		  Minibase.DiskManager.write_page(tempfd.pageno, bufpool[i]);
          
                //If the victim page is dirty, we set the reference count to 0
                 if((array.get(0).indexOf(pageno.pid)!=-1)&&(pageno.pid>8))
                   {
                     array.get(1).set(array.get(0).indexOf(pageno.pid), 0);
                     array.get(2).set(array.get(0).indexOf(pageno.pid), 0);
// some BHR code may go here
                    }
          }
          //reading the page from disk to the page given and pinning it. 
          if(skipRead)
        	  bufpool[i].copyPage(page);
          else
          	  Minibase.DiskManager.read_page(pageno, bufpool[i]);
          page.setPage(bufpool[i]);
// some BHR code may go here
      //Incrementing page faults
        pageFaults++;

        //If the requested page is not in the buffer, we load it from the disk and check if it is already
        //in our arraylist. If it is not in the arraylist, we add it to the arraylist an initialize page load count to 1 and page hit count to 0
if((array.get(0).indexOf(pageno.pid)==-1)&&(pageno.pid>8))
{
    array.get(0).add(pageno.pid);

    array.get(1).add(0);
    array.get(2).add(1);
    
  }
  //If the requested page is already in the arraylist, we simply increment the page load count
  else if(pageno.pid>8)
  {
    ix = array.get(0).indexOf(pageno.pid);
    increment(array.get(2), ix);
    
  }
	  }
	  	  //updating frame descriptor and notifying to replacer
	      tempfd.pageno.pid = pageno.pid;
          tempfd.pincnt = 1;
          tempfd.dirty = false;
          pagemap.put(Integer.valueOf(pageno.pid), tempfd);
          tempfd.state =Policy.PINNED;
      	  replacer.pinPage(tempfd);
   
  }

  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherrwise
   * @throws IllegalArgumentException if the page is not present or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) 
  {  
	  //the frame descriptor as the page is in the buffer pool 
	  FrameDesc tempfd = pagemap.get(Integer.valueOf(pageno.pid));
	  
	  //if page is not present an exception is thrown as "Page not present"
      if(tempfd == null)
          throw new IllegalArgumentException("Page not present");
      
      //if the page is present but not pinned an exception is thrown as "page not pinned"
      if(tempfd.pincnt == 0)
      {
          throw new IllegalArgumentException("Page not pinned");
      } 
      else
      {
    	  //unpinning the page by decrementing pincount and updating the frame descriptor and notifying replacer
          tempfd.pincnt--;
          tempfd.dirty = dirty;
          if(tempfd.pincnt== 0)
          tempfd.state = Policy.REFERENCED;
          replacer.unpinPage(tempfd);
          return;
      }
  }

  /**
   * Immediately writes a page in the buffer pool to disk, if dirty.
   */
  public void flushPage(PageId pageno) 
  {  
	  for(int i = 0; i < frametab.length; i++)
	 	  //checking for pageid or id the pageid is the frame descriptor and the dirty status of the page
          if((pageno == null || frametab[i].pageno.pid == pageno.pid) && frametab[i].dirty)
          {
        	  //writing down to disk if dirty status is true and updating dirty status of page to clean
              Minibase.DiskManager.write_page(frametab[i].pageno, bufpool[i]);
              frametab[i].dirty = false;
          }
  }

  /**
   * Immediately writes all dirty pages in the buffer pool to disk.
   */
  public void flushAllPages() 
  {
	  for(int i=0; i<frametab.length; i++) 
		  flushPage(frametab[i].pageno);
  }

  /**
   * Gets the total number of buffer frames.
   */
  public int getNumBuffers() 
  {
	  return bufpool.length;
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() 
  {
	  int numUnpinned = 0;
	  for(int i=0; i<frametab.length; i++) 
	  {
		  if(frametab[i].pincnt == 0)
			  numUnpinned++;
	  }
	  return numUnpinned;
  }
  
/*// Function to sort by column 
    public static void sortbyColumn(int arr[][], final int col) 
    { 
        // Using built-in sort function Arrays.sort 
        Arrays.sort(arr, new Comparator<int[]>() { 
            
          @Override              
          // Compare values according to columns 
          public int compare(final int[] entry1,  
                             final int[] entry2) { 
  
            // To sort in descending order revert  
            // the '>' Operator 
            if (entry1[col] > entry2[col]) 
                return -1; 
            else
                return 1; 
          } 
        });  // End of function call sort(). 
    }*/ 



    
    public void printBhrAndRefCount(){ 
    
    
    //print counts:
    int s1=0; int s2=0; int j;

    // Sorting the ArrayLists based on pageHits to find top k referenced pages
    for (int i = 0; i < array.get(1).size() - 1; i++) 
    {
        for (int k = array.get(2).size() - 1; k > i; k--) {
            if (array.get(1).get(k - 1) < array.get(1).get(k)) 
            {
                
                int temp1 = array.get(1).get(k - 1);
                array.get(1).set(k -1, array.get(1).get(k));
                array.get(1).set(k, temp1);

                int temp2 = array.get(0).get(k - 1);
                array.get(0).set(k -1, array.get(0).get(k));
                array.get(0).set(k, temp2);

                int temp3 = array.get(2).get(k - 1);
                array.get(2).set(k -1, array.get(2).get(k));
                array.get(2).set(k, temp3);
            }
         }
    }
    System.out.println("The top k (10) referenced pages are:\n");
     System.out.println("Page No.	No. of Loads 	 No. of Page Hits\n");
     for(j=0;j<array.get(0).size();j++)
     {
        if(array.get(0).get(j)<9)
        continue;
        System.out.print(array.get(0).get(j)+"\t\t\t");
        System.out.print(array.get(2).get(j)+"\t\t");
        System.out.print(array.get(1).get(j));
        System.out.print("\n");
     }
     System.out.println("+----------------------------------------+");

     //Calculating total page hits
    for(int i=0;i<array.get(1).size();i++){
        if(array.get(0).get(i)<9)
        continue;
        s1+=array.get(1).get(i);
    }
    totPageHits = (float)(s1);

    //Calculating total page loads
    for(int i=0;i<array.get(2).size();i++){
        if(array.get(0).get(i)<9)
        continue;
        s2+=array.get(2).get(i);
    }
    totPageRequests = (float)(s2);
    System.out.println("totPageHits: "+totPageHits);
    System.out.println("totPageRequests: "+totPageRequests);
    
    System.out.println("+----------------------------------------+");
   
    System.out.println("Page faults (policy dependent): "+pageFaults);
    System.out.println("+----------------------------------------+");
    
    

    //Calculating BHR for whole buffer
    aggregateBHR = totPageHits/totPageRequests;  //replace -1 with the formula  
  
    System.out.print("Aggregate BHR (for whole buffer): ");
    System.out.printf("%9.5f\n", aggregateBHR);
    
    System.out.println("+----------------------------------------+");
    
    // int s=replacer.second_chance();
    // System.out.println("The number of pages that have used their second chance are:" +s);



/*    //before sorting, need to compare the LAST refcounts and fix it
    for (int i = 0; i < pageRefCount.length ; i++) {
        if (pageRefCount[i][0] > pageRefCount[i][1]) pageRefCount[i][1] = pageRefCount[i][0];
        pageRefCount[i][0] = 0;
    }
    //Sort and print top k page references here. done by this code
    sortbyColumn(pageRefCount, 1);
    
    System.out.println("The top k (10) referenced pages are:");
    System.out.println("       Page No.\t\tNo. of references");
       
    for (int i = 0; i < 10 ; i++)    
      System.out.println("\t"+pageRefCount[i][2]+"\t\t"+pageRefCount[i][1]);
    
    System.out.println("+----------------------------------------+");
    //* System.out.println("pageRefCount.length: "+pageRefCount.length);
    // *for (int i = 0; i < pageRefCount.length ; i++)    
      // *System.out.println("\t"+pageRefCount[i][2]+"\t\t"+pageRefCount[i][1]+"\t\t"+pageRefCount[i][0]);*/
}

} // public class BufMgr implements GlobalConst
