package iterator;

import heap.*;
import global.*;
import java.io.*;

import BigT.Map;
/**
 * Jmap has the appropriate types.
 * Jmap already has its setHdr called to setup its vital stas.
 */

public class Projection
{
  /**
   *Map t1 and Map t2 will be joined, and the result 
   *will be stored in Map Jmap,before calling this mehtod.
   *we know that this two map can join in the common field
   *@param t1 The Map will be joined with t2
   *@param type1[] The array used to store the each attribute type
   *@param t2 The Map will be joined with t1
   *@param type2[] The array used to store the each attribute type
   *@param Jmap the returned Map
   *@param perm_mat[] shows what input fields go where in the output map
   *@param nOutFlds number of outer relation field 
   *@exception UnknowAttrType attrbute type does't match
   *@exception FieldNumberOutOfBoundException field number exceeds limit
   *@exception IOException some I/O fault 
   */
  public static void Join( Map  t1, AttrType type1[],
                           Map  t2, AttrType type2[],
        	           Map Jmap, FldSpec  perm_mat[], 
                           int nOutFlds
			   )
    throws UnknowAttrType,
	   FieldNumberOutOfBoundException,
	   IOException
    {
      
      
      for (int i = 0; i < nOutFlds; i++)
	{
	  switch (perm_mat[i].relation.key)
	    {
	    case RelSpec.outer:        // Field of outer (t1)
	      switch (type1[perm_mat[i].offset-1].attrType)
		{
		case AttrType.attrInteger:
		  Jmap.setIntFld(i+1, t1.getIntFld(perm_mat[i].offset));
		  break;
		case AttrType.attrReal:
		  Jmap.setFloFld(i+1, t1.getFloFld(perm_mat[i].offset));
		  break;
		case AttrType.attrString:
		  Jmap.setStrFld(i+1, t1.getStrFld(perm_mat[i].offset));
		  break;
		default:
		  
		  throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");
		  
		}
	      break;
	      
	    case RelSpec.innerRel:        // Field of inner (t2)
	      switch (type2[perm_mat[i].offset-1].attrType)
		{
		case AttrType.attrInteger:
		  Jmap.setIntFld(i+1, t2.getIntFld(perm_mat[i].offset));
		  break;
		case AttrType.attrReal:
		  Jmap.setFloFld(i+1, t2.getFloFld(perm_mat[i].offset));
		  break;
		case AttrType.attrString:
		  Jmap.setStrFld(i+1, t2.getStrFld(perm_mat[i].offset));
		  break;
		default:
		  
		  throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull");  
		  
		}
	      break;
	    }
	}
      return;
    }
  
  
  
  
  
  /**
   *Map t1 will be projected  
   *the result will be stored in Map Jmap
   *@param t1 The Map will be projected
   *@param type1[] The array used to store the each attribute type
   *@param Jmap the returned Map
   *@param perm_mat[] shows what input fields go where in the output map
   *@param nOutFlds number of outer relation field 
   *@exception UnknowAttrType attrbute type doesn't match
   *@exception WrongPermat wrong FldSpec argument
   *@exception FieldNumberOutOfBoundException field number exceeds limit
   *@exception IOException some I/O fault 
   */
  
  public static void Project(Map  t1, AttrType type1[], 
                             Map Jmap, FldSpec  perm_mat[], 
                             int nOutFlds
			     )
    throws UnknowAttrType,
	   WrongPermat,
	   FieldNumberOutOfBoundException,
	   IOException
    {
      
      
      for (int i = 0; i < nOutFlds; i++)
	{
	  switch (perm_mat[i].relation.key)
	    {
	    case RelSpec.outer:      // Field of outer (t1)
	      switch (type1[perm_mat[i].offset-1].attrType)
		{
		case AttrType.attrInteger:
		  Jmap.setIntFld(i+1, t1.getIntFld(perm_mat[i].offset));
		  break;
		case AttrType.attrReal:
		  Jmap.setFloFld(i+1, t1.getFloFld(perm_mat[i].offset));
		  break;
		case AttrType.attrString:
		  Jmap.setStrFld(i+1, t1.getStrFld(perm_mat[i].offset));
		  break;
		default:
		  
		  throw new UnknowAttrType("Don't know how to handle attrSymbol, attrNull"); 
	
		}
	      break;
	      
	    default:
	      
	      throw new WrongPermat("something is wrong in perm_mat");
	     
	    }
	}
      return;
    }
  
}


