package global;

public interface GlobalConst {

  public static final int MINIBASE_MAXARRSIZE = 50;

  // Here you need to change the buffer size.
  public static final int NUMBUF = 25;

  /** Size of page. */
  public static final int MINIBASE_PAGESIZE = 1024;           // in bytes

  /** Size of each frame. */
  public static final int MINIBASE_BUFFER_POOL_SIZE = 1024;   // in Frames

  public static final int MAX_SPACE = 1024;   // in Frames
  
  /**
   * in Pages => the DBMS Manager tells the DB how much disk 
   * space is available for the database.
   */
  public static final int MINIBASE_DB_SIZE = 10000;           
  public static final int MINIBASE_MAX_TRANSACTIONS = 100;
  public static final int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;
  
  /**
   * also the name of a relation
   */
  public static final int MAXFILENAME  = 15;          
  public static final int MAXINDEXNAME = 40;
  public static final int MAXATTRNAME  = 15;    
  public static final int MAX_NAME = 50;
  
  /**
   * added to make the BufMgr work correctly
   */

  public static final int INVALID_PAGE = -1;
  public static final int INVALID_PAGEID = -1;

  public static final boolean PIN_MEMCPY = true;
  public static final boolean PIN_DISKIO = false;
  

  public static final boolean UNPIN_CLEAN = false;
  public static final boolean UNPIN_DIRTY = true;

  
  /**
   * added for computing BHR 1 & 2 for the time being here
   * use these variable for storing the values; will be orinted by test cases
   */
   public static final int BHR_GLOBAL_MAX = 0;
   public static final int BHR_GLOBAL_MIN = 1;
   public static final int BHR_LOAD_BASED_MAX = 0;
   public static final int BHR_LOAD_BASED_MIN = 1;
   
   /**
   * added for computing top-k references for each policy
   * use these variable for storing the values; will be printed by test cases
   */
   //Change the following array dimension according to your need
   public static int[][] PAGE_LOADING_BASED_MAX_HITS = new int[1000][1000];
}
