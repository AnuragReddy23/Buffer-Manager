
package bufmgr;

import diskmgr.*;
import global.*;

  /**
   * class Policy is a subclass of class Replacer use the given replacement
   * policy algorithm for page replacement
   */
class FIFO extends  Replacer {   
//replace Policy above with impemented policy name (e.g., Lru, Clock)

  //
  // Frame State Constants
  //
  protected static final int AVAILABLE = 10;
  protected static final int REFERENCED = 11;
  protected static final int PINNED = 12;

  //Following are the fields required for LRU and MRU policies:
  /**
   * private field
   * An array to hold number of frames in the buffer pool
   */

    private int  frames[];
 
  /**
   * private field
   * number of frames used
   */   
  private int  nframes;
  int buf_size;
  /** Clock head; required for the default clock algorithm. */
  protected int head;

  /**
   * This pushes the given frame to the end of the list.
   * @param frameNo	the frame number
   */

  private void update1(int frameNo)
  {

    int i=frameNo;
    int temp = frames[frameNo];
    while(i>head)
    {
      frames[i]=frames[i-1];
      i--;
    }
    frames[head]=temp;
    head++;
    if(head==frametab.length)
    head=0;
  } 
  private void update2(int frameNo)
  {
    int i=frametab.length-1;
    int temp2=frames[i];
    int temp = frames[frameNo];
    while(i>head)
    {
      frames[i]=frames[i-1];
      i--;
    }
    int j=frameNo;
    while(j>0)
    {
      frames[j]=frames[j-1];
      j--;
    }
    frames[0]=temp2;
    frames[head]=temp;
    head++;
    if(head==frametab.length)
    head=0;

    }
    //System.out.println("Update 3");
     


  /**
   * Class constructor
   * Initializing frames[] pinter = null.
   */
    public FIFO(BufMgr mgrArg)
    {
      super(mgrArg);
      // initialize the frame states
    for (int i = 0; i < frametab.length; i++) {
      frametab[i].state = AVAILABLE;
    }
      // initialize parameters for LRU and MRU
    nframes = 0;
    frames = new int[frametab.length];
    buf_size = mgrArg.getNumBuffers();
    // initialize the clock head for Clock policy
    head = 0;
    }
  /**
   * Notifies the replacer of a new page.
   */
  public void newPage(FrameDesc fdesc) {
    // no need to update frame state
  }

  /**
   * Notifies the replacer of a free page.
   */
  public void freePage(FrameDesc fdesc) {
    fdesc.state = AVAILABLE;
  }

  /**
   * Notifies the replacer of a pined page.
   */
  public void pinPage(FrameDesc fdesc) {
    fdesc.state = PINNED;    
  }

  /**
   * Notifies the replacer of an unpinned page.
   */
  public void unpinPage(FrameDesc fdesc) {
    fdesc.state = AVAILABLE;
  }
  
  /**
   * Finding a free frame in the buffer pool
   * or choosing a page to replace using your policy
   *
   * @return 	return the frame number
   *		return -1 if failed
   */

 public int pickVictim()
 {
   //remove the below statement and write your code
  int a;
  //checking if the buffer has empty frames
  if(nframes<buf_size){
    a=nframes;
    nframes++;
    frametab[a].state=PINNED;
    frames[a]=a;
    return a;
  }
  //if the buffer is full, we have to replace the first unpinned page in the queue
  int b=head;
  while(b<buf_size){
    a=frames[b];
    if(frametab[a].state!=PINNED){
      frametab[a].state = PINNED;
    //System.out.print("Found Unpinned page");
      //sending the pinned frame to the end of the list again
      update1(b);
      return a;
    }
    ++b;
  }
  if(head>0){
    int i=0;
    while(i<head){
      a=frames[i];
      if(frametab[a].state!=PINNED){
        frametab[a].state = PINNED;
      //System.out.print("Found Unpinned page");
        //sending the pinned frame to the end of the list again
        update2(i);
        return a;
      }
      ++i;
    }
  }

  return -1;
 }
 }

