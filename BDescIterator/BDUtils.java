package BDescIterator;



import heap.*;
import global.*;
import java.io.*;
import java.lang.*;

import BigT.Map;

/**
 *some useful method when processing Map 
 */
public class BDUtils
{
	
	private static int compareTimeStamp(Map t1, Map t2)
	  {	
		  try
		  { 
			  int t1_f, t2_f;
			  t1_f = t1.getTimeStamp();				// Comparing timestamp
			  t2_f = t2.getTimeStamp();
			  int res;
			  res=t1_f-t2_f;
			  if (res <  0) return -1;
			  if (res >  0) return  1;
			  return  0;
		  }
		  
		  catch(Exception e)
		  {
			  System.out.println("Exception" +e);
			  return -2;
		  }

	  }
	private static int compareTimeStampDesc(Map t1, Map t2)
	  {	
		  try
		  { 
			  int t1_f, t2_f;
			  t1_f = t1.getTimeStamp();				// Comparing timestamp
			  t2_f = t2.getTimeStamp();
			  int res;
			  res=t2_f-t1_f;
			  if (res <  0) return -1;
			  if (res >  0) return  1;
			  return  0;
		  }
		  
		  catch(Exception e)
		  {
			  System.out.println("Exception" +e);
			  return -2;
		  }

	  }
	private static int compareRowLabel(Map t1, Map t2)
	  {	
		  try
		  { 
			  String t1_f, t2_f;
			  t1_f = t1.getRowLabel();			// Comparing row
			  t2_f = t2.getRowLabel();
			  
			  int t=t1_f.compareTo(t2_f);
			  if (t <  0) return -1;
			  if (t >  0) return  1;
			  return  0;
		  }
		  catch(Exception e)
		  {
			  System.out.println("Exception" +e);
			  return -2;
		  }

	  }
	 
	private static int compareColumnLabel(Map t1, Map t2)
	  {	
		  try
		  { 
			  String t1_f, t2_f;
			  t1_f = t1.getColumnLabel();			// Comparing column
			  t2_f = t2.getColumnLabel();
			  
			  int t=t1_f.compareTo(t2_f);
			  if (t <  0) return -1;
			  if (t >  0) return  1;
			  return  0;
		  }
		  catch(Exception e)
		  {
			  System.out.println("Exception" +e);
			  return -2;
		  }

	  }
	/*private static int compareValue(Map t1, Map t2)
	  {	
		  try
		  { System.out.println("comparing values");
			  String t1_f = null, t2_f=null;
			  int t_x,t_y;
			  t_x=Integer.parseInt(t1_f);
			  t_y=Integer.parseInt(t2_f);
			  t1_f = t1.getValue();			// Comparing column
			  t2_f = t2.getValue();
			  int t;
			  t=t_x-t_y;
			 System.out.println("value after comparing"+t);
			  if (t <  0) return -1;
			  if (t >  0) return  1;
			  return  0;
		  }
		  catch(Exception e)
		  {
			  System.out.println("Exception" +e);
			  return -2;
		  }

	  }*/
	private static int compareValue(Map t1, Map t2)
	  {	
		try
		  { 
			//  System.out.println("lol");
			  String t1_f, t2_f;
			  t1_f = t1.getValue();			// Comparing column
			  t2_f = t2.getValue();
			  while(t1_f .length()<25) {
				  t1_f ='0'+t1_f;
			 	}
			  while(t2_f .length()<25) {
				  t2_f ='0'+t2_f ;
			 	}
			  
			 int t=t1_f.compareTo(t2_f);
			  if (t <  0) return -1;
			  if (t >  0) return  1;
			  return  0;
		  }
		  catch(Exception e)
		  {
			  System.out.println("Exception" +e);
			  return -2;
		  }

	  }
	  
	
	public static int CompareMapWithMap(MapOrder orderType, Map t1, Map t2)
			  throws BDUtilsException
			  {
				  
				  int retVal = -2;
				//  System.out.println("number of order type"+orderType.mapOrder);

				  switch (orderType.mapOrder) 
				  {
				  case 1:              // R, C, T,V
					  retVal = compareRowLabel(t1, t2);
					  if (retVal==0) 
					  {
						  retVal = compareColumnLabel(t1, t2);
						  if(retVal==0) {
							  retVal = compareTimeStamp(t1, t2);
							  if(retVal==0){
								  retVal=compareValue(t1,t2);
							  }
							  
						  }
					  }
					  return retVal;
				  case 2:              // R, C, T
					  retVal = compareRowLabel(t1, t2);
					  if (retVal==0) 
					  {
						  retVal = compareColumnLabel(t1, t2);
						  if(retVal==0) {
							  retVal = compareTimeStamp(t1, t2);
							  
						  }
					  }
					  return retVal;
					  case 3:              // R, C, T
						  retVal = compareRowLabel(t1, t2);
						  if (retVal==0) 
						  {
							  retVal = compareColumnLabel(t1, t2);
							  if(retVal==0) {
								  retVal = compareTimeStamp(t1, t2);
								  
							  }
						  }
						  return retVal;
					  case 4:              // C, R,T
						  retVal = compareColumnLabel(t1, t2);
						  if (retVal==0) 
						  {
							  retVal = compareRowLabel(t1, t2);
							  if(retVal==0) {
								  retVal = compareTimeStamp(t1, t2);
								  
							  }
						  }
						  return retVal;
					  case 5:                		// R,T
						  retVal = compareRowLabel(t1, t2);
						  if (retVal==0) {
							  retVal = compareTimeStamp(t1, t2);
						  }
						  return retVal;
					  case 6:                		// C,T
						  retVal = compareColumnLabel(t1, t2);
						  if (retVal==0) {
							  retVal = compareTimeStamp(t1, t2);
						  }
						  return retVal;
					  case 7:                		// T
						  retVal = compareTimeStampDesc(t1, t2);
						  
						  return retVal;
					  case 8:                		// T
						  retVal = compareValue(t1, t2);
						  
						  return retVal;
					  
					  default: 
						  return retVal;
				  }
			  }
	
	
	
	
  
  /**
   * This function compares a map with another map in respective field, and
   *  returns:
   *
   *    0        if the two are equal,
   *    1        if the map is greater,
   *   -1        if the map is smaller,
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one map.
   *@param    t2        another map.
   *@param    t1_fld_no the field numbers in the maps to be compared.
   *@param    t2_fld_no the field numbers in the maps to be compared. 
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception BDUtilsException exception from this class
   *@return   0        if the two are equal,
   *          1        if the map is greater,
   *         -1        if the map is smaller,                              
   */
  public static int CompareMapWithMap(AttrType fldType,
					  Map  t1, int t1_fld_no,
					  Map  t2, int t2_fld_no)
    throws IOException,
	   UnknowAttrType,
	   BDUtilsException
    {
	 // System.out.println("in Compare with map :"+t1.getRowLabel()+"   "+t2.getRowLabel());
      int   t1_i,  t2_i;
      float t1_r,  t2_r;
      String t1_s, t2_s;
      
      switch (fldType.attrType) 
	{
	case AttrType.attrInteger:                // Compare two integers.
	  try {
	    t1_i = t1.getIntFld(t1_fld_no);
	    t2_i = t2.getIntFld(t2_fld_no);
	  }catch (FieldNumberOutOfBoundException e){
	    throw new BDUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
	  }
	  if (t1_i == t2_i) return  0;
	  if (t1_i <  t2_i) return -1;
	  if (t1_i >  t2_i) return  1;
	  
	case AttrType.attrReal:                // Compare two floats
	  try {
	    t1_r = t1.getFloFld(t1_fld_no);
	    t2_r = t2.getFloFld(t2_fld_no);
	  }catch (FieldNumberOutOfBoundException e){
	    throw new BDUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
	  }
	  if (t1_r == t2_r) return  0;
	  if (t1_r <  t2_r) return -1;
	  if (t1_r >  t2_r) return  1;
	  
	case AttrType.attrString:                // Compare two strings
	  try {
		//  System.out.println(t1_fld_no+"nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn");
	    t1_s = t1.getStrFld(t1_fld_no);
	    t2_s = t2.getStrFld(t2_fld_no);
	  }catch (FieldNumberOutOfBoundException e){
	    throw new BDUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
	  }
	  
	  // Now handle the special case that is posed by the max_values for strings...
	  if(t1_s.compareTo( t2_s)>0)return 1;
	  if (t1_s.compareTo( t2_s)<0)return -1;
	  return 0;
	default:
	  
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
    }
  
  
  
  /**
   * This function  compares  map1 with another map2 whose
   * field number is same as the map1
   *
   *@param    fldType   the type of the field being compared.
   *@param    t1        one map
   *@param    value     another map.
   *@param    t1_fld_no the field numbers in the maps to be compared.  
   *@return   0        if the two are equal,
   *          1        if the map is greater,
   *         -1        if the map is smaller,  
   *@exception UnknowAttrType don't know the attribute type   
   *@exception IOException some I/O fault
   *@exception BDUtilsException exception from this class   
   */            
  public static int CompareMapWithValue(AttrType fldType,
					  Map  t1, int t1_fld_no,
					  Map  value)
    throws IOException,
	   UnknowAttrType,
	   BDUtilsException
    {
      return CompareMapWithMap(fldType, t1, t1_fld_no, value, t1_fld_no);
    }
  
  /**
   *This function Compares two Map inn all fields 
   * @param t1 the first map
   * @param t2 the secocnd map
   * @param type[] the field types
   * @param len the field numbers
   * @return  0        if the two are not equal,
   *          1        if the two are equal,
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception BDUtilsException exception from this class
   */            
  
  public static boolean Equal(Map t1, Map t2, AttrType types[], int len)
    throws IOException,UnknowAttrType,BDUtilsException
    {
      int i;
      
      for (i = 1; i <= len; i++)
	if (CompareMapWithMap(types[i-1], t1, i, t2, i) != 0)
	  return false;
      return true;
    }
  
  /**
   *get the string specified by the field number
   *@param map the map 
   *@param fidno the field number
   *@return the content of the field number
   *@exception IOException some I/O fault
   *@exception BDUtilsException exception from this class
   */
  public static String Value(Map  map, int fldno)
    throws IOException,
	   BDUtilsException
    {
      String temp;
      try{
	temp = map.getStrFld(fldno);
      }catch (FieldNumberOutOfBoundException e){
	throw new BDUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
      }
      return temp;
    }
  
 
  /**
   *set up a map in specified field from a map
   *@param value the map to be set 
   *@param map the given map
   *@param fld_no the field number
   *@param fldType the map attr type
   *@exception UnknowAttrType don't know the attribute type
   *@exception IOException some I/O fault
   *@exception BDUtilsException exception from this class
   */  
  public static void SetValue(Map value, Map  map, int fld_no, AttrType fldType)
    throws IOException,
	   UnknowAttrType,
	   BDUtilsException
    {
      
      switch (fldType.attrType)
	{
	case AttrType.attrInteger:
	  try {
	    value.setIntFld(fld_no, map.getIntFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new BDUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
	  }
	  break;
	case AttrType.attrReal:
	  try {
	    value.setFloFld(fld_no, map.getFloFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new BDUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
	  }
	  break;
	case AttrType.attrString:
	  try {
	    value.setStrFld(fld_no, map.getStrFld(fld_no));
	  }catch (FieldNumberOutOfBoundException e){
	    throw new BDUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
	  }
	  break;
	default:
	  throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
	  
	}
      
      return;
    }
  public static void SetValue(Map value, Map  map)
		    throws IOException,
			   UnknowAttrType,
			   BDUtilsException
		    {
		      
	  value.mapCopy(map);
		      
		      return;
		    }
  
  /*
  *//**
   *set up the Jmap's attrtype, string size,field number for using join
   *@param Jmap  reference to an actual map  - no memory has been malloced
   *@param res_attrs  attributes type of result map
   *@param in1  array of the attributes of the map (ok)
   *@param len_in1  num of attributes of in1
   *@param in2  array of the attributes of the map (ok)
   *@param len_in2  num of attributes of in2
   *@param t1_str_sizes shows the length of the string fields in S
   *@param t2_str_sizes shows the length of the string fields in R
   *@param proj_list shows what input fields go where in the output map
   *@param nOutFlds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception BDUtilsException exception from this class
   *//*
  public static short[] setup_op_map(Map Jmap, AttrType[] res_attrs,
				       AttrType in1[], int len_in1, AttrType in2[], 
				       int len_in2, short t1_str_sizes[], 
				       short t2_str_sizes[], 
				       FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   BDUtilsException
    {
      short [] sizesT1 = new short [len_in1];
      short [] sizesT2 = new short [len_in2];
      int i, count = 0;
      
      for (i = 0; i < len_in1; i++)
        if (in1[i].attrType == AttrType.attrString)
	  sizesT1[i] = t1_str_sizes[count++];
      
      for (count = 0, i = 0; i < len_in2; i++)
	if (in2[i].attrType == AttrType.attrString)
	  sizesT2[i] = t2_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer)
	    res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
	  else if (proj_list[i].relation.key == RelSpec.innerRel)
	    res_attrs[i] = new AttrType(in2[proj_list[i].offset-1].attrType);
	}
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
            n_strs++;
	  else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
            n_strs++;
	}
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
            res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
	  else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
            res_str_sizes[count++] = sizesT2[proj_list[i].offset-1];
	}
      try {
	Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
      }catch (Exception e){
	throw new BDUtilsException(e,"setHdr() failed");
      }
      return res_str_sizes;
    }
  */
 /*
   *//**
   *set up the Jmap's attrtype, string size,field number for using project
   *@param Jmap  reference to an actual map  - no memory has been malloced
   *@param res_attrs  attributes type of result map
   *@param in1  array of the attributes of the map (ok)
   *@param len_in1  num of attributes of in1
   *@param t1_str_sizes shows the length of the string fields in S
   *@param proj_list shows what input fields go where in the output map
   *@param nOutFlds number of outer relation fileds
   *@exception IOException some I/O fault
   *@exception BDUtilsException exception from this class
   *@exception InvalidRelation invalid relation 
   *//*

  public static short[] setup_op_map(Map Jmap, AttrType res_attrs[],
				       AttrType in1[], int len_in1,
				       short t1_str_sizes[], 
				       FldSpec proj_list[], int nOutFlds)
    throws IOException,
	   BDUtilsException, 
	   InvalidRelation
    {
      short [] sizesT1 = new short [len_in1];
      int i, count = 0;
      
      for (i = 0; i < len_in1; i++)
        if (in1[i].attrType == AttrType.attrString)
	  sizesT1[i] = t1_str_sizes[count++];
      
      int n_strs = 0; 
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer) 
            res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
	  
	  else throw new InvalidRelation("Invalid relation -innerRel");
	}
      
      // Now construct the res_str_sizes array.
      for (i = 0; i < nOutFlds; i++)
	{
	  if (proj_list[i].relation.key == RelSpec.outer
	      && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
	    n_strs++;
	}
      
      short[] res_str_sizes = new short [n_strs];
      count         = 0;
      for (i = 0; i < nOutFlds; i++) {
	if (proj_list[i].relation.key ==RelSpec.outer
	    && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
	  res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
      }
     
      try {
    	//  System.out.println("in map utils:"+(short)nOutFlds+"  "+res_attrs+" " +res_str_sizes);
	Jmap.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
      }catch (Exception e){
	throw new BDUtilsException(e,"setHdr() failed");
      } 
      return res_str_sizes;
    }
  */
  


}




