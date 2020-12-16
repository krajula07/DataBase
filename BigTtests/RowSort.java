package BigTtests;



import diskmgr.PCounter;
import global.*;
import heap.Scan;
import iterator.Iterator;
import iterator.Sort;

import java.io.File;
import java.util.ArrayList;

import BDescIterator.BDIterator;
import BDescIterator.BDSort;
import BigT.BScan;
//import BigT.BigTHeapfile;
import BigT.Map;
import BigT.Stream;
import BigT.bigT;
import BigTIterator.BIterator;
import BigTIterator.BSort;
import btree.BTFileScan;
import btree.KeyDataEntry;



public class RowSort implements GlobalConst{
	 private final static boolean OK = true;
	    private final static boolean FAIL = false;
     static SystemDefs sysdef = null;
	public static String dbName;
	public static int Sortoption = 1;    //Index option
	public static bigT Result_HF = null;
	public static bigT Output_HF = null;

	public static bigT Final_HF=null;
	public static bigT Temp_HF=null;
	public static bigT Filtered_HF=null;
	public static bigT Closing_HF=null;
	static boolean RowLabel_null = false;
	static boolean ColumnLabel_null = false;
	static boolean TimeStamp_null = false;
	static boolean Value_null = false;
	boolean exists = false;
	public Scan Mapiter = null;
	public Sort mapSort = null;
	public int SORT_MAP_NUM_PAGES = 16;
	boolean scan_entire_heapfile = false;
	public String RowLabelFilter;
	public static String ColumnLabelFilter;
	public String ValueFilter;
	public int TimeStampFilter;
	public boolean scan_on_BT = false;
	public Map scan_on_BT_map = null;
	public static int count=1;
	public static ArrayList<String> filtered_values = new ArrayList<String>();
	// Takes the query as argument and executes the required functionalities
    public static  void runRowSort(String[] arguments) throws Exception {

        // arguments[0] = "rowsort"
        // arguments[1] = "INBTNAME"
        // arguments[2] = "OUTBTNAME"
        // arguments[3] = "COLUMNNAME"//string
        // arguments[4] = "numbuf"
    	//arguments[5]="type";
    	
    	
        PCounter.initialize();

        boolean status = OK;
        
        String bigTable1 = arguments[1];
       // int type = Integer.parseInt(arguments[5]);
       // String BigTdbname = dbName+"_"+type;
        int numBuf = Integer.parseInt(arguments[5]);
        int sortorder = Integer.parseInt(arguments[3]);
        String outputFile=arguments[2];
      
        String columnFilter = arguments[4];
       
        
        BigTDBManager bdb = new BigTDBManager();
        bdb.init("BigDB",numBuf);
        
        bigT bhf_1=null,bhf_2=null,bhf_3=null,bhf_4=null,bhf_5=null;

     //   System.out.println(BigTdbname);
        bigT bigT = null;
        //////////////////////////////////////////
		try 
		  { 
			  bhf_1 = new bigT(bigTable1+"_1"); 
			  bhf_2 = new bigT(bigTable1+"_2"); 
			  bhf_3 = new bigT(bigTable1+"_3"); 
			  bhf_4 = new bigT(bigTable1+"_4"); 
			  bhf_5 = new bigT(bigTable1+"_5"); 
			  bigT= new bigT("rowsort");
			//  bigT.deleteBigt();
			//  bigT= new bigT("rowsort");
		  } 
		  catch (Exception e) 
		  {
			  e.printStackTrace(); 
		  }
		
		  MID mid = new MID();
		  Map map = new Map();
		  BScan bscan=null;
		  //int count1=0;
		  if(bhf_1.getMapCnt()>0) {
			  System.out.println(bhf_1._fileName+" has "+bhf_1.getMapCnt()+" maps");
			  bscan = bhf_1.openScan();
			  while((map = bscan.getNext(mid)) != null) {
				  bigT.insertMap(map.getMapByteArray());
			  }
		  }
		  if(bhf_2.getMapCnt()>0) {
			  System.out.println(bhf_2._fileName+" has "+bhf_2.getMapCnt()+" maps");
			  bscan = bhf_2.openScan();
			  while((map = bscan.getNext(mid)) != null) {
				  bigT.insertMap(map.getMapByteArray());
			  }
		  }
		  if(bhf_3.getMapCnt()>0) {
			  System.out.println(bhf_3._fileName+" has "+bhf_3.getMapCnt()+" maps");
			  bscan = bhf_3.openScan();
			  while((map = bscan.getNext(mid)) != null) {
				  bigT.insertMap(map.getMapByteArray());
			  }
		  }
		  if(bhf_4.getMapCnt()>0) {
			  System.out.println(bhf_4._fileName+" has "+bhf_4.getMapCnt()+" maps");
			  bscan = bhf_4.openScan();
			  while((map = bscan.getNext(mid)) != null) {
				  bigT.insertMap(map.getMapByteArray());
			  }
		  }
		  if(bhf_5.getMapCnt()>0) {
			  System.out.println(bhf_5._fileName+" has "+bhf_5.getMapCnt()+" maps");
			  bscan = bhf_5.openScan();
			  while((map = bscan.getNext(mid)) != null) {
				  bigT.insertMap(map.getMapByteArray());
			  }
			  
		  }
		  bscan.closescan();
//		////////////////////////////////////////////
//        File file = new File(dbName);
//
//        
//        if (file.exists()) { 
//        try {
//        	bigT = new bigT(BigTdbname);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        }
//        else{
//       	System.out.println("Heap file doesnt exists");
//        }
        if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("* The heap file has left pages pinned\n");
            status = FAIL;
        }

        System.out.println("Heap file contains "+bigT.getMapCnt()+" maps");
        try {
        	Filtered_HF = new bigT("filteredFile");
        //	System.out.println("into file");
        } catch (Exception e) {
            e.printStackTrace();
        }
        String result1="Result";
		Result_HF = new bigT(result1);	
		String output=outputFile;
		String temp1="tempFile";
		Output_HF = new bigT(temp1);
		String final1="finalFile";
		Final_HF=new bigT(final1);
		
		Temp_HF=new bigT(output+"_1");
		Temp_HF.deleteBigt();
		Temp_HF=new bigT(output+"_1");
		
        RowSortStream(bigT,columnFilter,outputFile,sortorder); // To retrieve maps from the queried big table

        	 try
     		{
     			if(Result_HF != null )
     			{
     				Result_HF.deleteFile();
     			}
     			if(Filtered_HF != null )
     			{
     				Filtered_HF.deleteFile();
     			}
     			if(Output_HF != null )
     			{
     				Output_HF.deleteFile();
     			}
     			
     			if(Final_HF != null )
     			{
     				Final_HF.deleteFile();
     			}
     			if(bigT != null )
     			{
     				bigT.deleteFile();
     			}
     		
     		}
     		catch(Exception e)
     		{
     				System.out.println("Error closing Stream"+e);
     		}
         

        bdb.close();
        
        Util.printStatInfo();
      
    }
    public static  void RowSortStream(bigT bigHeapFile,String columnFilter,String outputFile,int sortorder) 
    		throws Exception
    		{
    					
    			
    			
  	        ScanEntirebigT(bigHeapFile,columnFilter,outputFile);

  	        BIterator sort = Util.createSortIteratorForMap(Result_HF,3);
   		           
  	        Map tuple1=null;
  	        Map tuple = sort.get_next();
  	        Map temp3=null;
  	        Map temp2=new Map();
  	        temp2.setRowLabel(tuple.getRowLabel());
  	        temp2.setColumnLabel(tuple.getColumnLabel());
  	        temp2.setTimeStamp(tuple.getTimeStamp());
  	        temp2.setValue(tuple.getValue());

    		            while((tuple1=sort.get_next())!=null) {
    		            	
    		            	
    		            String s=temp2.getRowLabel();
    		            String s1=tuple1.getRowLabel();
    		         
    		               if(s.compareTo(s1)!=0){
    		            	try {
  		                	//System.out.println(temp2.getRowLabel()+","+ temp2.getColumnLabel()+","+(temp2.getTimeStamp())+","+temp2.getValue());
  		                	Filtered_HF.insertMap(temp2.getMapByteArray());
    		                }
    		                catch (Exception e) {
    		                    e.printStackTrace();
    		                }
    		            }
    		             temp2.setRowLabel(tuple1.getRowLabel());
    		             temp2.setColumnLabel(tuple1.getColumnLabel());
    		             temp2.setTimeStamp(tuple1.getTimeStamp());
    		             temp2.setValue(tuple1.getValue());
    		               
    		            }
    		            //System.out.println(temp2.getRowLabel()+","+ temp2.getColumnLabel()+","+(temp2.getTimeStamp())+","+temp2.getValue());
    		            Filtered_HF.insertMap(temp2.getMapByteArray());
    	          if(sortorder==2){
    	        	  
    	        	  
    	        	  BDSort sortd = Util.createSortIteratorDescForMap(Filtered_HF,6);      
    	        	  Map tupleValue=null;
    	        	  while((tupleValue=sortd.get_next())!=null)
    	        	  {//System.out.println(tupleValue.getRowLabel());
    	        	  	FinalScanEntirebigT(bigHeapFile,tupleValue.getRowLabel(),outputFile,tupleValue.getColumnLabel());
    	        	  //	filtered_values.add(tupleValue.getRowLabel());
    	        	  //ScanEntirebigT(bigHeapFile,tupleValue.getRowLabel(),outputFile);
    	        	  }
    	        	  sortd.close();
    	          }
    	          else{
BIterator sortv = Util.createSortIteratorForMap(Filtered_HF,6);      
Map tupleValue=null;
while((tupleValue=sortv.get_next())!=null)
{//System.out.println(tupleValue.getRowLabel());
	FinalScanEntirebigT(bigHeapFile,tupleValue.getRowLabel(),outputFile,tupleValue.getColumnLabel());
	//filtered_values.add(tupleValue.getRowLabel());
//ScanEntirebigT(bigHeapFile,tupleValue.getRowLabel(),outputFile);
}
sortv.close();  }
BScan am_final = new BScan(Output_HF); // Creating a scanning object on Map_HF
Map map_final = null;
MID mid_final=new MID();
while((map_final = (Map) am_final.getNext(mid_final)) != null)
{
	Final_HF.insertMap(map_final.getMapByteArray());
}
    		            
    		            //System.out.println("distinct row count"+Filtered_HF.getMapCnt());

    		          
     		           
BScan am_scan = new BScan(Output_HF); // Creating a scanning object on Map_HF
Map map_scan = null;
MID mid_scan=new MID();
while((map_scan = (Map) am_scan.getNext(mid_scan)) != null)
{
	if((map_scan.getRowLabel().compareTo("none"))!=0){
		System.out.println(map_scan.getRowLabel()+","+map_scan.getColumnLabel()+","+map_scan.getTimeStamp()+","+map_scan.getValue());
		Temp_HF.insertMap(map_scan.getMapByteArray());
	}
	//System.out.println(map_scan.getRowLabel()+","+map_scan.getColumnLabel()+","+map_scan.getTimeStamp()+","+map_scan.getValue());
}
/*BScan am_scan1 = new BScan(Temp_HF); // Creating a scanning object on Map_HF
Map map_scan1 = null;
MID mid_scan1=new MID();
while((map_scan1 = (Map) am_scan1.getNext(mid_scan1)) != null)
{
	
	System.out.println(map_scan1.getRowLabel()+","+map_scan1.getColumnLabel()+","+map_scan1.getTimeStamp()+","+map_scan1.getValue());
}*/
System.out.println("final_rowfiltered count>>>>>>>>>>>>>>>>>"+Temp_HF.getMapCnt());
 //System.out.println("OutPut count>>>>>>>>>>>>"+Output_HF.getMapCnt()); 		
 
am_scan.closescan();
//am_scan1.closescan();
sort.close();
//sortv.close();
am_final.closescan();
    				
    			   
    			}
    		
private static void ScanEntirebigT(bigT bigHeapFile,String columnFilter,String outputFile)
    			{
    				try
    				{
    					
    					ColumnLabelFilter = columnFilter;
    			
    					boolean result = true;
    					
    					String rowlabel = null, columnlabel = null, value = null;
    					
    					bigT Map_HF = bigHeapFile;
   
    					
    					
    		            BScan am = new BScan(Map_HF); // Creating a scanning object on Map_HF
    		            Map map = null;
    		 
    		            MID mid=new MID();
    		         
    		            while((map = (Map) am.getNext(mid)) != null)
    		            {
    		            	//System.out.println(map.getLength()+": map length in row sort");
    		            	result = true;
    						rowlabel=map.getRowLabel();
    						columnlabel=map.getColumnLabel();
    						value=map.getValue();
    						
    						
    						if(!ColumnLabel_null && result)
    						{
    							result=columnLabelCheck(columnFilter, columnlabel);	
    						}
    						
    						if(result)
    						{					
    							Result_HF.insertMap(map.getMapByteArray());
    							//System.out.println(map.getRowLabel()+","+ map.getColumnLabel()+","+(map.getTimeStamp())+","+map.getValue());
    						}
    						else{
    							Output_HF.insertMap(map.getMapByteArray());
    						}
    		            }
    		            
    		            am.closescan();
    				}
    				catch(Exception e)
    				{
    					System.err.println ("Error scanning entire heap file for query::"+e);
    					e.printStackTrace();
    					Runtime.getRuntime().exit(1);
    				}
    			}
    			
    			
    			
private static void FinalScanEntirebigT(bigT bigHeapFile,String rowFilter,String outputFile,String columnFilter)
    			{
    				try
    				{
    					
    					ColumnLabelFilter = rowFilter;
    			
    					boolean result = true;
    					
    					String rowlabel = null, columnlabel = null, value = null;
    					
    					bigT Map_HF = bigHeapFile;
    					
    					
    					
    					/*String output=outputFile;
    					Output_HF = new bigT(output);
    				*/
    					
    					
    						BScan am_toDelete = new BScan(Output_HF); // Creating a scanning object on Map_HF
        		            Map map_toDelete = null;
        		 
        		            MID mid_todelete=new MID();
        		            
        		         //System.out.println(mid_todelete.pageNo+","+mid_todelete.pageNo.pid+","+mid_todelete.slotNo);
        		            while((map_toDelete = (Map) am_toDelete.getNext(mid_todelete)) != null)
        		            {
        		            	result = true;
        						rowlabel=map_toDelete.getRowLabel();
        						columnlabel=map_toDelete.getColumnLabel();
        						value=map_toDelete.getValue();

        						if(!ColumnLabel_null && result)
        						{
        							result=columnLabelCheck(rowFilter, rowlabel);	
        						}
        						
        						if(result)
        						{			
        							Map tempX=new Map();
        							tempX.setRowLabel("none");
        							tempX.setColumnLabel("none");
        							tempX.setTimeStamp(7777777);
        							tempX.setValue("7777777");
        							Output_HF.updateMap(mid_todelete, tempX);
        						}
 
        		            }
        		            am_toDelete.closescan();
        		            
    					
    		            BScan am = new BScan(Map_HF); // Creating a scanning object on Map_HF
    		            Map map = null;
    		 
    		            MID mid=new MID();
    		          //  System.out.println(mid.pageNo+","+mid.slotNo);
    		            while((map = (Map) am.getNext(mid)) != null)
    		            {
    		            	result = true;
    						rowlabel=map.getRowLabel();
    						columnlabel=map.getColumnLabel();
    						value=map.getValue();
    					
    						if(!ColumnLabel_null && result)
    						{
    							result=columnLabelCheck(rowFilter, rowlabel);	
    							
    						}
    						if(result)
    						{	System.out.println(map.getRowLabel()+","+map.getColumnLabel()+","+map.getTimeStamp()+","+map.getValue());		
    							//Output_HF.insertMap(map.getMapByteArray());
    						Temp_HF.insertMap(map.getMapByteArray());
    						}
 		
    		            }
    		            
    		            am.closescan();
    				}
    				catch(Exception e)
    				{
    					System.err.println ("Error scanning entire heap file for query::"+e);
    					e.printStackTrace();
    					Runtime.getRuntime().exit(1);
    				}
    				
    			}
    			
    			
public static Boolean columnLabelCheck(String columnFilter, String columnlabel) {
    				
    				Boolean result=true;
    				
    				if(columnFilter.startsWith("[")) {
    					
    					String leftString=null;
    					String rightString=null;
    					StringBuilder str = new StringBuilder(columnFilter);
    					String[] strarray=((str.deleteCharAt(str.length()-1)).deleteCharAt(0)).toString().split(",");
    					
    					if(!strarray[0].equalsIgnoreCase("null"))
    						leftString=strarray[0];
    					if(!strarray[1].equalsIgnoreCase("null"))
    						rightString=strarray[1];				
    					if(leftString==null && rightString!=null) 
    						result = result && (columnlabel.compareTo(rightString) <= 0);
    					else if(leftString!=null && rightString==null)
    						result = result && (leftString.compareTo(columnlabel) <= 0);
    					else {
    						result = result && (leftString.compareTo(columnlabel) <= 0) && (columnlabel.compareTo(rightString) <= 0);
    					}
    					}
    				else					
    					result = result && (columnFilter.compareTo(columnlabel) == 0);	
    				
    				return result;
    				
    			}
}