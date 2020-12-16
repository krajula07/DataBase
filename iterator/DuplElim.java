package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;

import java.lang.*;

import BigT.Map;

import java.io.*;

/**
 *Eleminate the duplicate maps from the input relation
 */
public class DuplElim extends Iterator
{
  private AttrType[] _in;     // memory for array allocated by constructor
  private short       in_len;
  private short[]    str_lens;
  
  private Iterator _am;
  private boolean      done;
  
  private AttrType  sortFldType;
  private int       sortFldLen;
  private Map    Jmap;
  
  private Map TempMap1, TempMap2;
  
  /**
   *Constructor to set up some information.
   *@param in[]  Array containing field types of R.
   *@param len_in # of columns in R.
   *@param s_sizes[] store the length of string appeared in map
   *@param am input relation iterator, access method for left input to join,
   *@param amt_of_mem the page numbers required IN PAGES
   *@exception IOException some I/O fault
   *@exception DuplElimException the exception from DuplElim.java
   */
  public DuplElim(
		  AttrType in[],         
		  short      len_in,     
		  short    s_sizes[],
		  Iterator am,          
		  int       amt_of_mem,  
		  boolean     inp_sorted
		  )throws IOException ,DuplElimException
    {
      _in = new AttrType[in.length];
      System.arraycopy(in,0,_in,0,in.length);
      in_len = len_in;
     
      Jmap =  new Map();
      try {
	Jmap.setHdr(len_in, _in, s_sizes);
      }catch (Exception e){
	throw new DuplElimException(e, "setHdr() failed");
      }
     
      sortFldType = in[0];
      switch (sortFldType.attrType)
	{
	case AttrType.attrInteger:
	  sortFldLen = 4;
	  break;
	case AttrType.attrReal:
	  sortFldLen = 4;
	  break;
	case AttrType.attrString:
	  sortFldLen = s_sizes[0];
	  break;
	default:
	  //error("Unknown type");
	  return;
	}
      
      _am = am;
      MapOrder order = new MapOrder(MapOrder.Ascending);
//      if (!inp_sorted)
//	{
//	  try {
//	    _am = new Sort(in, len_in, s_sizes, am, 1, order,
//			   sortFldLen, amt_of_mem);
//	  }catch(SortException e){
//	    e.printStackTrace();
//	    throw new DuplElimException(e, "SortException is caught by DuplElim.java");
//	  }
//	}

      // Allocate memory for the temporary maps
      TempMap1 =  new Map();
      TempMap2 = new Map();
      try{
	TempMap1.setHdr(in_len, _in, s_sizes);
	TempMap2.setHdr(in_len, _in, s_sizes);
      }catch (Exception e){
	throw new DuplElimException(e, "setHdr() failed");
      }
      done = false;
    }

  /**
   * The map is returned.
   *@return call this function to get the map
   *@exception JoinsException some join exception
   *@exception IndexException exception from super class    
   *@exception IOException I/O errors
   *@exception InvalidMapSizeException invalid map size
   *@exception InvalidTypeException map type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception MapUtilsException exception from using map utilities
   *@exception PredEvalException exception from PredEval class
   *@exception SortException sort exception
   *@exception LowMemException memory error
   *@exception UnknowAttrType attribute type unknown
   *@exception UnknownKeyTypeException key type unknown
   *@exception Exception other exceptions
   */
  public Map get_next() 
    throws IOException,
	   JoinsException ,
	   IndexException,
	   InvalidMapSizeException,
	   InvalidTypeException, 
	   PageNotReadException,
	   MapUtilsException, 
	   PredEvalException,
	   SortException,
	   LowMemException,
	   UnknowAttrType,
	   UnknownKeyTypeException,
	   Exception
    {
      Map t;
      
      if (done)
        return null;
      Jmap.mapCopy(TempMap1);
     
      do {
	if ((t = _am.get_next()) == null) {
	  done = true;                    // next call returns DONE;
	  return null;
	} 
	TempMap2.mapCopy(t);
      } while (MapUtils.Equal(TempMap1, TempMap2, _in, in_len));
      
      // Now copy the the TempMap2 (new o/p map) into TempMap1.
      TempMap1.mapCopy(TempMap2);
      Jmap.mapCopy(TempMap2);
      return Jmap ;
    }
 
  /**
   * implement the abstract method close() from super class Iterator
   *to finish cleaning up
   *@exception JoinsException join error from lower layers
   */
  public void close() throws JoinsException
    {
      if (!closeFlag) {
	
	try {
	  _am.close();
	}catch (Exception e) {
	  throw new JoinsException(e, "DuplElim.java: error in closing iterator.");
	}
	closeFlag = true;
      }
    }  
}
