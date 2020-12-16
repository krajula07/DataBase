package BigTIterator;

import java.io.*;

import BigT.BScan;
import BigT.Map;

import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import index.*;
import iterator.MapUtils;

import chainexception.*;

/**
 * The Sort class sorts a file. All necessary information are passed as 
 * arguments to the constructor. After the constructor call, the user can
 * repeatly call <code>get_next()</code> to get maps in sorted order.
 * After the sorting is done, the user should call <code>close()</code>
 * to clean up.
 */
public class BSort extends BIterator implements GlobalConst
{
  private static final int ARBIT_RUNS = 20;
  
  private AttrType[]  _in;         
  private short       n_cols;
  private short[]     str_lens;
  private BScan    _am;
  private Scan sc;
  private int         _sort_fld;
  private MapOrder  order;
  private int         _n_pages;
  private byte[][]    bufs;
  private boolean     first_time;
  private int         Nruns;
  private int         max_elems_in_heap;
  private int         sortFldLen;
  private int         map_size=54;
  
  private BpNodeSplayPQ Q;
  private Heapfile[]   temp_files; 
  private int          n_tempfiles;
  private Map        output_map;  
  private int[]        n_maps;
  private int          n_runs;
  private Map        op_buf;
  private BOBuf         o_buf;
  private BSpoofIbuf[]  i_buf;
  private PageId[]     bufs_pids;
  private boolean useBM = true; // flag for whether to use buffer manager
  
  /**
   * Set up for merging the runs.
   * Open an input buffer for each run, and insert the first element (min)
   * from each run into a heap. <code>delete_min() </code> will then get 
   * the minimum of all runs.
   * @param map_size size (in bytes) of each map
   * @param n_R_runs number of runs
   * @exception IOException from lower layers
   * @exception LowMemException there is not enough memory to 
   *                 sort in two passes (a subclass of SortException).
   * @exception SortException something went wrong in the lower layer. 
   * @exception Exception other exceptions
   */
  private void setup_for_merge(int map_size, int n_R_runs)
    throws IOException, 
	   LowMemException, 
	   BSortException,
	   Exception
  {
    // don't know what will happen if n_R_runs > _n_pages
    if (n_R_runs > _n_pages) 
      throw new LowMemException("Sort.java: Not enough memory to sort in two passes."); 

    int i;
    Bpnode cur_node;  // need pq_defs.java
    
    i_buf = new BSpoofIbuf[n_R_runs];   // need io_bufs.java
    for (int j=0; j<n_R_runs; j++) i_buf[j] = new BSpoofIbuf();
    
    // construct the lists, ignore TEST for now
    // this is a patch, I am not sure whether it works well -- bingjie 4/20/98
    
    for (i=0; i<n_R_runs; i++) {
      byte[][] apage = new byte[1][];
      apage[0] = bufs[i];

      // need iobufs.java
      i_buf[i].init(temp_files[i], apage, 1,map_size, n_maps[i]);

      cur_node = new Bpnode();
      cur_node.run_num = i;
      
      // may need change depending on whether Get() returns the original
      // or make a copy of the map, need io_bufs.java ???
      Map temp_map = new Map(map_size);

      try {
	temp_map.setHdr(n_cols, _in, str_lens);
      }
      catch (Exception e) {
	throw new BSortException(e, "Sort.java: Map.setHdr() failed");
      }
      
      temp_map =i_buf[i].Get(temp_map);  // need io_bufs.java
            
      if (temp_map != null) {
	/*
	System.out.print("Get map from run " + i);
	temp_map.print(_in);
	*/
	cur_node.map = temp_map; // no copy needed
	try {
	  Q.enq(cur_node);
	}
	catch (UnknowAttrType e) {
	  throw new BSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	}
	catch (BUtilsException e) {
	  throw new BSortException(e, "Sort.java: MapUtilsException caught from Q.enq()");
	}

      }
    }
    return; 
  }
  
  
  private Map createminDummyLastElement()
  { 
    PageId pageno = new PageId(-1);
        
    char[] c = new char[1];
    c[0] = Character.MIN_VALUE; 
    String s = new String(c);
    Map map = new Map();
    try {
    	map.setRowLabel(s);
    	map.setColumnLabel(s);
    	map.setTimeStamp(Integer.MIN_VALUE);
    	map.setValue(s);
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
	
	return map;
  }
  private Map createmaxDummyLastElement()
  { 
    PageId pageno = new PageId(-1);
        
    char[] c = new char[1];
    c[0] = Character.MAX_VALUE;
    String s = new String(c);
    Map map = new Map();
    try {
    	map.setRowLabel(s);
    	map.setColumnLabel(s);
    	map.setTimeStamp(Integer.MAX_VALUE);
    	map.setValue(s);
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    
	
	return map;
  }
  
  /**
   * Generate sorted runs.
   * Using heap sort.
   * @param  max_elems    maximum number of elements in heap
   * @param  sortFldType  attribute type of the sort field
   * @param  sortFldLen   length of the sort field
   * @return number of runs generated
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   * @exception JoinsException from <code>Iterator.get_next()</code>
   */
  private int generate_runs(int max_elems, AttrType sortFldType, int sortFldLen) 
    throws IOException, 
	   BSortException, 
	   UnknowAttrType,
	   BUtilsException,
	   
	   Exception
  {
    Map map; 
    Bpnode cur_node;
    BpNodeSplayPQ Q1 = new BpNodeSplayPQ(order);
    BpNodeSplayPQ Q2 = new BpNodeSplayPQ(order);
    BpNodeSplayPQ pcurr_Q = Q1;
    BpNodeSplayPQ pother_Q = Q2; 
    Map lastElem = new Map(map_size);  // need map.java
    try {
      lastElem.setHdr(n_cols, _in, str_lens);
    }
    catch (Exception e) {
      throw new BSortException(e, "Sort.java: setHdr() failed");
    }
    
    int run_num = 0;  // keeps track of the number of runs

    
    int p_elems_curr_Q = 0;
    int p_elems_other_Q = 0;
    
    int comp_res;
    lastElem=createminDummyLastElement();
    
    
    // maintain a fixed maximum number of elements in the heap
    while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
      try {
    	  MID mid=new MID();
	map = _am.getNext(mid);  // according to Iterator.java
	//byte[] temparray=map.getMapByteArray();
	//System.out.println(temparray.length+" in sort ppppppppppppppppppppp");
      } catch (Exception e) {
	e.printStackTrace(); 
	throw new BSortException(e, "Sort.java: get_next() failed");
      } 
      
      if (map == null) {
    	 // System.out.println("kavya map is null");
	break;
      }
      cur_node = new Bpnode();
      cur_node.map = new Map(map); // map copy needed --  Bingjie 4/29/98 
//System.out.println("checking the values in map:"+cur_node.map.getColumnLabel()+":"+cur_node.map.getRowLabel()+":"+cur_node.map.getTimeStamp()+":"+cur_node.map.getValue());
      pcurr_Q.enq(cur_node);
      p_elems_curr_Q ++;
    }
  //  System.out.println("before queue.................."+p_elems_curr_Q);
    
    // now the queue is full, starting writing to file while keep trying
    // to add new maps to the queue. The ones that does not fit are put
    // on the other queue temperarily
    while (true) {
   
      cur_node = pcurr_Q.deq();
    if (cur_node == null) break; 
      p_elems_curr_Q --;
     
      
      
      
      
     // System.out.println("order:"+order+"last element:"+lastElem.getRowLabel()+":"+lastElem.getColumnLabel()+":"+lastElem.getTimeStamp()+":"+lastElem.getValue()+"current node"+cur_node.run_num+";"+cur_node.map.getRowLabel()+":"+cur_node.map.getColumnLabel()+":"+cur_node.map.getTimeStamp()+":"+cur_node.map.getValue());
      
      comp_res = BUtils.CompareMapWithMap(order, cur_node.map, lastElem);  // need
    
      
      
      
      
     // System.out.println("compare result:"+comp_res);
      
      
   // if ((comp_res < 0 && order.mapOrder == MapOrder.Ascending) || (comp_res > 0 && order.mapOrder == MapOrder.Descending)) {
    if((comp_res<0)){
	// doesn't fit in current run, put into the other queue
	try {
		//System.out.println("another queue..............");
	  pother_Q.enq(cur_node);
	}
	catch (UnknowAttrType e) {
	  throw new BSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	}
	//System.out.println("fits into the same queue");
	p_elems_other_Q ++;
      }
      else {
	// set lastElem to have the value of the current map,
	// need map_utils.java
    //	  System.out.println("in else part.......................................................");
	//BUtils.SetValue(lastElem, cur_node.map, _sort_fld, sortFldType);
	BUtils.SetValue(lastElem,cur_node.map);
	
	o_buf.Put(cur_node.map);
      }
    // System.out.println("elements in other Q+"+p_elems_other_Q);
      
      // check whether the other queue is full
      if (p_elems_other_Q == max_elems) {
	// close current run and start next run
	n_maps[run_num] = (int) o_buf.flush();  // need io_bufs.java
	run_num ++;

	//System.out.println("run number and tempheapfiles"+run_num+":"+n_tempfiles);
	// check to see whether need to expand the array
	if (run_num == n_tempfiles) {
	  Heapfile[] temp1 = new Heapfile[2*n_tempfiles];
	  for (int i=0; i<n_tempfiles; i++) {
	    temp1[i] = temp_files[i];
	  }
	  temp_files = temp1; 
	  n_tempfiles *= 2; 

	  int[] temp2 = new int[2*n_runs];
	  for(int j=0; j<n_runs; j++) {
	    temp2[j] = n_maps[j];
	  }
	  n_maps = temp2;
	  n_runs *=2; 
	}
	
	try {
		//System.out.println(run_num+":run number");
	    temp_files[run_num] = new Heapfile(null);
	}
	catch (Exception e) {
	  throw new BSortException(e, "Sort.java: create Heapfile failed");
	}
	
	// need io_bufs.java
	o_buf.init(bufs, _n_pages, map_size, temp_files[run_num], false);
	lastElem=createminDummyLastElement();
	
	
    
	// switch the current heap and the other heap
	BpNodeSplayPQ tempQ = pcurr_Q;
	pcurr_Q = pother_Q;
	pother_Q = tempQ;
	int tempelems = p_elems_curr_Q;
	p_elems_curr_Q = p_elems_other_Q;
	p_elems_other_Q = tempelems;
      }
      
      // now check whether the current queue is empty
      else if (p_elems_curr_Q == 0) {
	while ((p_elems_curr_Q + p_elems_other_Q) < max_elems) {
	  try {
		  MID mid=new MID();
	    map = _am.getNext(mid);  // according to Iterator.java
	  } catch (Exception e) {
	    throw new BSortException(e, "get_next() failed");
	  } 
	  
	  if (map == null) {
	    break;
	  }
	  cur_node = new Bpnode();
	  cur_node.map = new Map(map); // map copy needed --  Bingjie 4/29/98 

	  try {
	    pcurr_Q.enq(cur_node);
	  }
	  catch (UnknowAttrType e) {
	    throw new BSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	  }
	  p_elems_curr_Q ++;
	}
      }
      
      // Check if we are done
      if (p_elems_curr_Q == 0) {
	// current queue empty despite our attemps to fill in
	// indicating no more maps from input
	if (p_elems_other_Q == 0) {
	  // other queue is also empty, no more maps to write out, done
	  break; // of the while(true) loop
	}
	else {
	  // generate one more run for all maps in the other queue
	  // close current run and start next run
	  n_maps[run_num] = (int) o_buf.flush();  // need io_bufs.java
	  run_num ++;
	  
	  // check to see whether need to expand the array
	  if (run_num == n_tempfiles) {
	    Heapfile[] temp1 = new Heapfile[2*n_tempfiles];
	    for (int i=0; i<n_tempfiles; i++) {
	      temp1[i] = temp_files[i];
	    }
	    temp_files = temp1; 
	    n_tempfiles *= 2; 
	    
	    int[] temp2 = new int[2*n_runs];
	    for(int j=0; j<n_runs; j++) {
	      temp2[j] = n_maps[j];
	    }
	    n_maps = temp2;
	    n_runs *=2; 
	  }

	  try {
	    temp_files[run_num] = new Heapfile(null); 
	  }
	  catch (Exception e) {
	    throw new BSortException(e, "Sort.java: create Heapfile failed");
	  }
	  
	  // need io_bufs.java
	  o_buf.init(bufs, _n_pages, map_size, temp_files[run_num], false);
	  lastElem=createminDummyLastElement();
	  // set the last Elem to be the minimum value for the sort field
	  /*if(order.mapOrder == MapOrder.Ascending) {
	    try {
	      MIN_VAL(lastElem, sortFldType);
	    } catch (UnknowAttrType e) {
	      throw new BSortException(e, "Sort.java: UnknowAttrType caught from MIN_VAL()");
	    } catch (Exception e) {
	      throw new BSortException(e, "MIN_VAL failed");
	    } 
	  }
	  else {
	    try {
	      MAX_VAL(lastElem, sortFldType);
	    } catch (UnknowAttrType e) {
	      throw new BSortException(e, "Sort.java: UnknowAttrType caught from MAX_VAL()");
	    } catch (Exception e) {
	      throw new BSortException(e, "MIN_VAL failed");
	    } 
	  }*/
	
	  // switch the current heap and the other heap
	  BpNodeSplayPQ tempQ = pcurr_Q;
	  pcurr_Q = pother_Q;
	  pother_Q = tempQ;
	  int tempelems = p_elems_curr_Q;
	  p_elems_curr_Q = p_elems_other_Q;
	  p_elems_other_Q = tempelems;
	}
      } // end of if (p_elems_curr_Q == 0)
    } // end of while (true)

    // close the last run
    n_maps[run_num] = (int) o_buf.flush();
    run_num ++;
    
    return run_num; 
  }
  
  /**
   * Remove the minimum value among all the runs.
   * @return the minimum map removed
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  private Map delete_min() 
    throws IOException, 
	   BSortException,
	   Exception
  {
    Bpnode cur_node;                // needs pq_defs.java  
    Map new_map, old_map;  

    cur_node = Q.deq();
   // System.out.println(cur_node.map.getColumnLabel());
    old_map = cur_node.map;
    /*
    System.out.print("Get ");
    old_map.print(_in);
    */
    // we just removed one map from one run, now we need to put another
    // map of the same run into the queue
    if (i_buf[cur_node.run_num].empty() != true) { 
      // run not exhausted 
      new_map = new Map(map_size); // need map.java??

      try {
	new_map.setHdr(n_cols, _in, str_lens);
      }
      catch (Exception e) {
	throw new BSortException(e, "Sort.java: setHdr() failed");
      }
      
      new_map = i_buf[cur_node.run_num].Get(new_map);  
      if (new_map != null) {
	
	//System.out.print(" fill in from run " + cur_node.run_num);
	//new_map.print(_in);
	
	cur_node.map = new_map;  // no copy needed -- I think Bingjie 4/22/98
	//System.out.println(" "+new_map.getColumnLabel());
	try {
	  Q.enq(cur_node);
	} catch (UnknowAttrType e) {
	  throw new BSortException(e, "Sort.java: UnknowAttrType caught from Q.enq()");
	} catch (BUtilsException e) {
	  throw new BSortException(e, "Sort.java: MapUtilsException caught from Q.enq()");
	} 
      }
      else {
	throw new BSortException("********** Wait a minute, I thought input is not empty ***************");
      }
      
    }

    // changed to return Map instead of return char array ????
    //System.out.println(old_map.getColumnLabel());
    return old_map; 
  }
  
  /**
   * Set lastElem to be the minimum value of the appropriate type
   * @param lastElem the map
   * @param sortFldType the sort field type
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MIN_VAL(Map lastElem, AttrType sortFldType) 
    throws IOException, 
	   FieldNumberOutOfBoundException,
	   UnknowAttrType {

    //    short[] s_size = new short[Map.max_size]; // need Map.java
    //    AttrType[] junk = new AttrType[1];
    //    junk[0] = new AttrType(sortFldType.attrType);
    char[] c = new char[1];
    c[0] = Character.MIN_VALUE; 
    String s = new String(c);
    //    short fld_no = 1;
    
    switch (sortFldType.attrType) {
    case AttrType.attrInteger: 
      //      lastElem.setHdr(fld_no, junk, null);
      lastElem.setIntFld(_sort_fld, Integer.MIN_VALUE);
      break;
    case AttrType.attrReal:
      //      lastElem.setHdr(fld-no, junk, null);
      lastElem.setFloFld(_sort_fld, Float.MIN_VALUE);
      break;
    case AttrType.attrString:
      //      lastElem.setHdr(fld_no, junk, s_size);
      lastElem.setStrFld(_sort_fld, s);
      break;
    default:
      // don't know how to handle attrSymbol, attrNull
      //System.err.println("error in sort.java");
      throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }
    
    return;
  }

  /**
   * Set lastElem to be the maximum value of the appropriate type
   * @param lastElem the map
   * @param sortFldType the sort field type
   * @exception IOException from lower layers
   * @exception UnknowAttrType attrSymbol or attrNull encountered
   */
  private void MAX_VAL(Map lastElem, AttrType sortFldType) 
    throws IOException, 
	   FieldNumberOutOfBoundException,
	   UnknowAttrType {

    //    short[] s_size = new short[Map.max_size]; // need Map.java
    //    AttrType[] junk = new AttrType[1];
    //    junk[0] = new AttrType(sortFldType.attrType);
    char[] c = new char[1];
    c[0] = Character.MAX_VALUE; 
    String s = new String(c);
    //    short fld_no = 1;
    
    switch (sortFldType.attrType) {
    case AttrType.attrInteger: 
      //      lastElem.setHdr(fld_no, junk, null);
      lastElem.setIntFld(_sort_fld, Integer.MAX_VALUE);
      break;
    case AttrType.attrReal:
      //      lastElem.setHdr(fld_no, junk, null);
      lastElem.setFloFld(_sort_fld, Float.MAX_VALUE);
      break;
    case AttrType.attrString:
      //      lastElem.setHdr(fld_no, junk, s_size);
      lastElem.setStrFld(_sort_fld, s);
      break;
    default:
      // don't know how to handle attrSymbol, attrNull
      //System.err.println("error in sort.java");
      throw new UnknowAttrType("Sort.java: don't know how to handle attrSymbol, attrNull");
    }
    
    return;
  }
  
  /** 
   * Class constructor, take information about the maps, and set up 
   * the sorting
   * @param in array containing attribute types of the relation
   * @param len_in number of columns in the relation
   * @param str_sizes array of sizes of string attributes
   * @param am an iterator for accessing the maps
   * @param sort_fld the field number of the field to sort on
   * @param sort_order the sorting order (ASCENDING, DESCENDING)
   * @param sort_field_len the length of the sort field
   * @param n_pages amount of memory (in pages) available for sorting
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  public BSort(AttrType[] in,         
	      short      len_in,             
	      short[]    str_sizes,
	      BScan   am,                 
	      int        sort_fld,          
	      MapOrder sort_order,     
	      int        sort_fld_len,  
	      int        n_pages      
	      ) throws IOException, BSortException
  {
    _in = new AttrType[len_in];
    n_cols = len_in;
    int n_strs = 0;

    for (int i=0; i<len_in; i++) {
      _in[i] = new AttrType(in[i].attrType);
      if (in[i].attrType == AttrType.attrString) {
	n_strs ++;
      } 
    }
    
    str_lens = new short[n_strs];
    
    n_strs = 0;
    for (int i=0; i<len_in; i++) {
      if (_in[i].attrType == AttrType.attrString) {
	str_lens[n_strs] = str_sizes[n_strs];
	n_strs ++;
      }
    }
    
    Map t = new Map(); // need Map.java
    try {
      t.setHdr(len_in, _in, str_sizes);
    }
    catch (Exception e) {
      throw new BSortException(e, "Sort.java: t.setHdr() failed");
    }
    map_size = 54;
    
    _am = am;
    _sort_fld = sort_fld;
    order = sort_order;
    _n_pages = n_pages;
    
    // this may need change, bufs ???  need io_bufs.java
    //    bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
    bufs_pids = new PageId[_n_pages];
    bufs = new byte[_n_pages][];

    if (useBM) {
      try {
	get_buffer_pages(_n_pages, bufs_pids, bufs);
      }
      catch (Exception e) {
	throw new BSortException(e, "Sort.java: BUFmgr error");
      }
    }
    else {
      for (int k=0; k<_n_pages; k++) bufs[k] = new byte[MAX_SPACE];
    }
    
    first_time = true;
    
    // as a heuristic, we set the number of runs to an arbitrary value
    // of ARBIT_RUNS
    temp_files = new Heapfile[ARBIT_RUNS];
    n_tempfiles = ARBIT_RUNS;
    n_maps = new int[ARBIT_RUNS]; 
    n_runs = ARBIT_RUNS;

    try {
      temp_files[0] = new Heapfile(null);
//      System.out.println(temp_files[0].getRecCnt()+"record count in sort");
    }
    catch (Exception e) {
      throw new BSortException(e, "Sort.java: Heapfile error");
    }
    
    o_buf = new BOBuf();
    
    o_buf.init(bufs, _n_pages, map_size, temp_files[0], false);
    //    output_map = null;
    
    max_elems_in_heap = 10000;
    sortFldLen = sort_fld_len;
    
    Q = new BpNodeSplayPQ(order);

    op_buf = new Map(map_size);   // need Map.java
    try {
      op_buf.setHdr(n_cols, _in, str_lens);
    }
    catch (Exception e) {
      throw new BSortException(e, "Sort.java: op_buf.setHdr() failed");
    }
    //System.out.println("end");
  }
  
  /**
   * Returns the next map in sorted order.
   * Note: You need to copy out the content of the map, otherwise it
   *       will be overwritten by the next <code>get_next()</code> call.
   * @return the next map, null if all maps exhausted
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   * @exception JoinsException from <code>generate_runs()</code>.
   * @exception UnknowAttrType attribute type unknown
   * @exception LowMemException memory low exception
   * @exception Exception other exceptions
   */
  public Map get_next() 
    throws IOException, 
	   BSortException, 
	   UnknowAttrType,
	   LowMemException, 
	   
	   Exception
  {
    if (first_time) {
      // first get_next call to the sort routine
      first_time = false;
      
      // generate runs
      Nruns = generate_runs(max_elems_in_heap, _in[_sort_fld-1], sortFldLen);
           // System.out.println("Generated " + Nruns + " runs");
      
      // setup state to perform merge of runs. 
      // Open input buffers for all the input file
      setup_for_merge(map_size, Nruns);
    }
    
    if (Q.empty()) {  
      // no more maps availble
      return null;
    }
    
    output_map = delete_min();
    //System.out.println(output_map.getColumnLabel()+"in merge runs");
    if (output_map != null){
      op_buf.mapCopy(output_map);
     // System.out.println(op_buf.getColumnLabel()+"in if part");
      return op_buf; 
    }
    else 
      return null; 
  }
/*  public Sort(Scan am, MapOrder sort_order, int n_pages) 
		  throws IOException,SortException
		  {
			  sc = am;
			  order = sort_order;
			  _n_pages = n_pages;
			  MID mid=new MID();
Map map=null;
try {
	if((map = (Map) am.getNext(mid)) != null){
		System.out.println("in sorttttttttttttttttttttttttttttt..............."+map.getColumnLabel());
	}
} catch (InvalidMapSizeException e1) {
	// TODO Auto-generated catch block
	e1.printStackTrace();
}
			  // this may need change, bufs ???  need io_bufs.java
			  // bufs = get_buffer_pages(_n_pages, bufs_pids, bufs);
			  bufs_pids = new PageId[_n_pages];
			  bufs = new byte[_n_pages][];

			  if (useBM) 
			  {
				  try 
				  {
					  get_buffer_pages(_n_pages, bufs_pids, bufs);
				  }
				  catch (Exception e) 
				  {
					  throw new SortException(e, "Map Sort.java: BUFmgr error");
				  }
			  }
			  else 
			  {
				  for (int k=0; k<_n_pages; k++) 
				  {
					  bufs[k] = new byte[MAX_SPACE];
				  }
			  }

			  first_time = true;

			  // as a heuristic, we set the number of runs to an arbitrary value
			  // of ARBIT_RUNS
			  temp_files = new Heapfile[ARBIT_RUNS];
			  n_tempfiles = ARBIT_RUNS;
			  n_maps = new int[ARBIT_RUNS]; 
			  n_runs = ARBIT_RUNS;

			  try 
			  {
				  temp_files[0] = new Heapfile(null);
			  }
			  catch (Exception e) 
			  {
				  throw new SortException(e, "Map Sort.java: MapHeapfile error");
			  }

			  o_buf = new OBuf();

			 o_buf.init(bufs, _n_pages, temp_files[0], false);
			  //    output_tuple = null;
			 // o_buf.init(bufs, n_pages, tSize, temp_fd, buffer);

			  max_elems_in_heap = 200;

			  Q = new pnodeSplayPQ(order);
			  
			  op_buf = new Map();
		  }*/
  /**
   * Cleaning up, including releasing buffer pages from the buffer pool
   * and removing temporary files from the database.
   * @exception IOException from lower layers
   * @exception SortException something went wrong in the lower layer. 
   */
  public void close() throws BSortException, IOException
  {
    // clean up
    if (!closeFlag) {
       
      try {
	_am.closescan();
      }
      catch (Exception e) {
	throw new BSortException(e, "Sort.java: error in closing iterator.");
      }

      if (useBM) {
	try {
	  free_buffer_pages(_n_pages, bufs_pids);
	} 
	catch (Exception e) {
	  throw new BSortException(e, "Sort.java: BUFmgr error");
	}
	for (int i=0; i<_n_pages; i++) bufs_pids[i].pid = INVALID_PAGE;
      }
      
      for (int i = 0; i<temp_files.length; i++) {
	if (temp_files[i] != null) {
	  try {
	    temp_files[i].deleteFile();
	  }
	  catch (Exception e) {
	    throw new BSortException(e, "Sort.java: Heapfile error");
	  }
	  temp_files[i] = null; 
	}
      }
      closeFlag = true;
    } 
  } 

}


