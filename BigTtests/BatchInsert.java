package BigTtests;

import diskmgr.PCounter;
import global.GlobalConst;
import global.SystemDefs;
import heap.HFException;



public class BatchInsert implements GlobalConst {

	public static void runBatchInsert(String[] arguments) throws HFException, Exception {

        // arguments[0] = "batchinsert"
        // arguments[1] = "Data file path"
        // arguments[2] = "TYPE"
    	// arguments[3] = "BIGTABLENAME"
    	// arguments[4] = "Number of Buffers"

        PCounter.initialize(); // Counter intialization
        
        String dataFilePath = arguments[1];
        int indexType = Integer.parseInt(arguments[2]);
        String dbName = arguments[3];
        int NUMBUFF=Integer.parseInt(arguments[4]);
        
       //Check if the DataBase already exists and if not create one
        BigTDBManager bdb = new BigTDBManager();
        bdb.init("BigDB",indexType, NUMBUFF);
           
        (SystemDefs.JavabaseDB).createIndex(dataFilePath,dbName,indexType);
        
        bdb.close();
        
        if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers() !=
  			  SystemDefs.JavabaseBM.getNumBuffers()) {
  			  System.err.println("* There are left pages pinned\n");  }
        
		Util.printStatInfo();
        }
    
   }
        

