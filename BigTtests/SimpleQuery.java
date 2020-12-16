package BigTtests;



import diskmgr.PCounter;
import global.*;
import heap.InvalidTypeException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

//import BigT.BigTHeapfile;
import BigT.Map;
import BigT.RowJoin;
import BigT.Stream;
import BigT.bigT;
import BigTIterator.BIterator;
import BigTIterator.BSortException;
import BigTIterator.BUtilsException;
import BigTIterator.LowMemException;
import BigTIterator.UnknowAttrType;
import BigTIterator.UnknownKeyTypeException;
import bufmgr.PageNotReadException;



public class SimpleQuery implements GlobalConst{
	
    private final static boolean OK = true;
    private final static boolean FAIL = false;
    static boolean RowLabel_null = false;
	static boolean ColumnLabel_null = false;
	static boolean TimeStamp_null = false;
	static boolean Value_null = false;
	public static bigT Result_HFile ;
	
	// Takes the query as argument and executes the required functionalities
    public static void runSimpleNodeQuery(String[] arguments) throws Exception {

        // query BIGTABLENAME ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF
    	
        PCounter.initialize();

        boolean status = OK;
        
        //String dbName = arguments[1];
       // int type = Integer.parseInt(arguments[2]);
        //String BigTdbname=dbName+"_"+type;
        
        String bigTableName = arguments[1];
        int numBuf = Integer.parseInt(arguments[6]);
        
        int ordertype = Integer.parseInt(arguments[2]);
        String rowFilter=arguments[3];
        String columnFilter=arguments[4];
        String valueFilter=arguments[5];
        
        BigTDBManager bdb = new BigTDBManager();
        bdb.init("BigDB",1,numBuf);

//        System.out.println("File name:- "+ BigTdbname);
//        File file = new File(dbName);
//
//        bigT bigT = null;
//        if (file.exists()) { 
//        try {
//        	bigT = new bigT(BigTdbname,type);
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

  //      System.out.println("File contains "+bigT.getMapCnt()+" maps");
        
        //Need to call bigT and send the file as parameters (name,type)
        // Stream stream= SystemDefs.JavabaseDB.openStream(dbName,ordertype,rowFilter,columnFilter,valueFilter);
        Stream stream=new Stream(bigTableName,ordertype,rowFilter,columnFilter,valueFilter); // To retrieve maps from the queried big table
        
        // Map fmap=null;
         //MID fmid=null;
         
		//         while((fmap=stream.getNext(fmid))!=null)
		//         {
		//        	 System.out.println(fmap.getRowLabel() +" "+fmap.getColumnLabel()+" " +fmap.getTimeStamp() +" -> " + fmap.getValue());
		//         }
        
         if(stream!=null) stream.closeStream();

        bdb.close();
        
        Util.printStatInfo();
      
    }
   
    public static void runJoinQuery(String[] arguments) throws Exception {
    	
   	 PCounter.initialize();
   	 
   	String bigT1=arguments[1];
   	String bigT2=arguments[2];
   	String outBigT=arguments[3];
   	String columnFilter=arguments[4];
   	int numbuf=Integer.parseInt(arguments[5]);
   	
    BigTDBManager bdb = new BigTDBManager();
    bdb.init("BigDB",1,numbuf);

   	RowJoin r=new RowJoin(numbuf,bigT1,bigT2,outBigT,columnFilter);
   	
   	bdb.close();
   	
   	 Util.printStatInfo();
   }

	public static String getDistinctRowLabels(bigT mergeBigT) throws InvalidTypeException, PageNotReadException, BUtilsException, BSortException, 
	LowMemException, UnknowAttrType, UnknownKeyTypeException, IOException, Exception {
		
		BIterator sort = Util.createSortIteratorForMap(mergeBigT,3);
		
		ArrayList<String> allData = new ArrayList<String>();
		int rowLabel = 0;
		Map tuple = null;
        try {
           // MID mid=new MID();
            tuple = sort.get_next();
            allData.add(tuple.getRowLabel());
        } catch (Exception e) {
            e.printStackTrace();
        }
        while((tuple = sort.get_next()) != null) {
            try {
            	allData.add(tuple.getRowLabel());
            }
             catch (Exception e) {
                e.printStackTrace();
            }
        }
        sort.close();
        int size = allData.size();
        for(int i = 0; i < size-1; i++) {
        	String data1 = allData.get(i);
        	String data2 = allData.get(i+1);
        	if(data1 != data2) {
        		rowLabel++;
        	}
        }
        rowLabel++;
        String result = rowLabel + "";
		return result;
	}

}