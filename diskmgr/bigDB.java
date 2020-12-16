package diskmgr;

import java.io.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import BigT.BScan;
import BigT.Map;
import BigT.Stream;
import BigT.bigT;
import BigTIterator.BIterator;
import BigTIterator.BSortException;
import BigTIterator.BUtilsException;
import BigTIterator.LowMemException;
import BigTIterator.UnknowAttrType;
import BigTIterator.UnknownKeyTypeException;
import BigTtests.BigTDBManager;
import BigTtests.Util;
import heap.*;
import bufmgr.*;
import global.*;
import btree.*;

public class bigDB extends DB implements GlobalConst {

	private Heapfile TEMP_Map_HF, Map_HF; // Temporary heap file for sorting //Maps Heap file to store maps
	private BTreeFile Map_BTree, Map_BTreeIndex;// BTree Map file on Map Heap file
	private static BTreeFile Map_BTreeIndex_insert;
	private static BTreeFile Map_BTreeIndex_Subinsert;
	private static BTreeFile Map_BTreeIndex_StorageIndex5;
	private static String curr_dbname; // BigT Database name
	private static String bigTFileName;
	private static String dataFilePath;
	private final static boolean OK = true, FAIL = false;
	public static bigT bhf, bhf_1, bhf_2, bhf_3, bhf_4, bhf_5, file1, file2, file3, file4, file5;
	private static int rowLabelCount = 0, columnLabelCount = 0, Total_Maps = 0;
	private static BTreeFile Map_BTreeIndex_StorageIndex1, Map_BTreeIndex_StorageIndex2, Map_BTreeIndex_StorageIndex3,
			Map_BTreeIndex_StorageIndex4;

	/**
	 * Default Constructor
	 */

	public bigDB() {
	}

	public void bigDB(int type) {
		int keytype = AttrType.attrInteger;
		try {
			TEMP_Map_HF = new Heapfile("tempresult");
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try {
			Map_HF = new Heapfile(curr_dbname + "/mapHF");
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try {
			Map_BTree = new BTreeFile(curr_dbname + "/mapBT", keytype, 255, 1);
			Map_BTree.close();
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		try {
			Map_BTreeIndex = new BTreeFile(curr_dbname + "/Map_BTreeIndex", keytype, 255, 1);
			Map_BTreeIndex.close();
		} catch (Exception e) {
			System.err.println("Error creating B tree index for given index option" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}

	public void openbigDB(String dbname)
			throws IOException, InvalidPageNumberException, FileIOException, DiskMgrException {
		curr_dbname = new String(dbname);
		try {
			openDB(dbname);
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}

	public void openbigDB(String dbname, int num_pages, int type)
			throws IOException, InvalidPageNumberException, FileIOException, DiskMgrException {
		this.curr_dbname = new String(dbname);
		bigTFileName = curr_dbname + "_" + type;
		try {
			openDB(dbname, num_pages);
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}

	public void closebigDB()
			throws PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException, ReplacerException {
		try {
			if (Map_BTree != null) {
				Map_BTree.close();
			}
			if (Map_BTreeIndex != null) {
				Map_BTreeIndex.close();
			}
			if (TEMP_Map_HF != null) {
				TEMP_Map_HF.deleteFile();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public BTreeFile getMap_BTreeIndex() throws GetFileEntryException, PinPageException, ConstructPageException {
		Map_BTreeIndex = new BTreeFile(curr_dbname + "/Map_BTreeIndex");
		return Map_BTreeIndex;
	}

	public BTreeFile getMap_BTree() throws GetFileEntryException, PinPageException, ConstructPageException {
		Map_BTree = new BTreeFile(curr_dbname + "/mapBT");
		return Map_BTree;
	}

	public int getMapCnt() {
		try {
			Map_HF = new Heapfile(curr_dbname + "/mapHF");
			Total_Maps = Map_HF.getRecCnt();
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		return Total_Maps;
	}

	public static int getUniqueRowLabelCnt() {
		return bigDB.rowLabelCount;
	}

	public static int getUniqueColumnLabelCnt() {
		return bigDB.columnLabelCount;
	}

	public Stream openStream(String dbName, int ordertype, String rowFilter, String columnFilter, String valueFilter)
			throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		Stream stream = null;
		bhf = new bigT(dbName + "_" + ordertype);
		try {
			stream = bhf.openStream(ordertype, rowFilter, columnFilter, valueFilter);
		} catch (Exception e) {
			System.out.println("Error in openStream of bigDB");
		}
		return stream;
	}

	public void insertMap(String[] mapQuery) {
		bigTFileName = mapQuery[6] + "_" + mapQuery[5];
	}

	public void createIndex(String dataFilePath, String dbName, int indexType) throws HFException, Exception {
		this.bigTFileName = dbName;
		this.dataFilePath = dataFilePath;
		switch (indexType) {
		case 0:
			// IndexType0();
			break;

		default:

			createbigTHeapFile(indexType, null, null);
			break;
		}
	}

	public static void createbigTHeapFile(int indexType, String fileName, Map singleMap)
			throws FileIOException, InvalidPageNumberException, DiskMgrException, IOException,
			InvalidSlotNumberException, InvalidMapSizeException, HFDiskMgrException, HFBufMgrException,
			KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
			ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
			DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
			ScanIteratorException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException, FreePageException, DeleteFileEntryException, FileAlreadyDeletedException, HFException,
			GetFileEntryException, AddFileEntryException {

		boolean status = OK;
		String line = "";
		BufferedReader br = null;
		ArrayList<String> data = new ArrayList<String>();

		if (fileName != null) {
			bigTFileName = fileName;
			data.add(singleMap.getRowLabel() + ":" + singleMap.getColumnLabel() + ":"
					+ Util.padLeftZeros(singleMap.getTimeStamp(), 25) + ":" + singleMap.getValue() + ":" + indexType);
		} else {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(dataFilePath), "UTF-8"));
		}
		try {
			bhf_1 = new bigT(bigTFileName + "_1");
			bhf_2 = new bigT(bigTFileName + "_2");
			bhf_3 = new bigT(bigTFileName + "_3");
			bhf_4 = new bigT(bigTFileName + "_4");
			bhf_5 = new bigT(bigTFileName + "_5");
		} catch (Exception e) {
			e.printStackTrace();
		}

		MID mid = new MID();
		Map map = new Map();

		if (bhf_1.getMapCnt() > 0) {
			BScan bscan = bhf_1.openScan();
			while ((map = bscan.getNext(mid)) != null) {
				data.add(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
						+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue() + ":" + 1);
			}
			bscan.closescan();
			bhf_1.deleteBigt();
			bhf_1 = new bigT(bigTFileName + "_1");
		}
		if (bhf_2.getMapCnt() > 0) {
			BScan bscan = bhf_2.openScan();
			while ((map = bscan.getNext(mid)) != null) {
				data.add(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
						+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue() + ":" + 2);
			}
			bscan.closescan();
			bhf_2.deleteBigt();
			bhf_2 = new bigT(bigTFileName + "_2");
		}
		if (bhf_3.getMapCnt() > 0) {
			BScan bscan = bhf_3.openScan();
			while ((map = bscan.getNext(mid)) != null) {
				data.add(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
						+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue() + ":" + 3);
			}
			bscan.closescan();
			bhf_3.deleteBigt();
			bhf_3 = new bigT(bigTFileName + "_3");
		}
		if (bhf_4.getMapCnt() > 0) {
			BScan bscan = bhf_4.openScan();
			while ((map = bscan.getNext(mid)) != null) {
				data.add(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
						+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue() + ":" + 4);
			}
			bscan.closescan();
			bhf_4.deleteBigt();
			bhf_4 = new bigT(bigTFileName + "_4");
		}
		if (bhf_5.getMapCnt() > 0) {
			BScan bscan = bhf_5.openScan();
			while ((map = bscan.getNext(mid)) != null) {
				data.add(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
						+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue() + ":" + 5);
			}
			bscan.closescan();
			bhf_5.deleteBigt();
			bhf_5 = new bigT(bigTFileName + "_5");
		}

		if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {
			System.err.println("* The heap file has left pages pinned\n");
			status = FAIL;
		}

		try {
			Map_BTreeIndex_insert = new BTreeFile(bigTFileName + "/BTIndex", AttrType.attrString, 109, 0);
			Map_BTreeIndex_Subinsert = new BTreeFile(bigTFileName + "/BTSubIndex", AttrType.attrString, 109, 0);
			Map_BTreeIndex_StorageIndex1 = new BTreeFile(bigTFileName + "/BTIndex1", AttrType.attrString, 109, 0);
			Map_BTreeIndex_StorageIndex2 = new BTreeFile(bigTFileName + "/BTIndex2", AttrType.attrString, 109, 0);
			Map_BTreeIndex_StorageIndex3 = new BTreeFile(bigTFileName + "/BTIndex3", AttrType.attrString, 109, 0);
			Map_BTreeIndex_StorageIndex4 = new BTreeFile(bigTFileName + "/BTIndex4", AttrType.attrString, 109, 0);
			Map_BTreeIndex_StorageIndex5 = new BTreeFile(bigTFileName + "/BTIndex5", AttrType.attrString, 109, 0);
		}

		catch (GetFileEntryException | ConstructPageException | AddFileEntryException e1) {
			e1.printStackTrace();
		}

		System.err.println(LocalDateTime.now());

		data.forEach(item -> {
			KeyClass key = new StringKey(item);
			try {
				Map_BTreeIndex_insert.insert(key, mid);
			} catch (KeyTooLongException | KeyNotMatchException | LeafInsertRecException | IndexInsertRecException
					| ConstructPageException | UnpinPageException | PinPageException | NodeNotMatchException
					| ConvertException | DeleteRecException | IndexSearchException | IteratorException
					| LeafDeleteException | InsertException | IOException e) {
				e.printStackTrace();
			}
		});

		if (fileName == null) {
			while ((line = br.readLine()) != null) {
				String[] strs = line.replaceAll("[^a-zA-Z0-9_,]", "").trim().split(",");
				String mapStr = (strs[0] + ":" + strs[1] + ":" + Util.padLeftZeros(strs[3], 25) + ":" + strs[2] + ":"
						+ indexType);
				KeyClass key = new StringKey(mapStr);
				Map_BTreeIndex_insert.insert(key, mid);
				data.add(key.toString());
			}
			br.close();
		}

		BTFileScan scan = Map_BTreeIndex_insert.new_scan(null, null);

		ArrayList<String> temp1 = new ArrayList<String>();
		KeyDataEntry entry = null;
		while ((entry = scan.get_next()) != null) {
			temp1.add(entry.key.toString());
		}

		scan.DestroyBTreeFileScan();
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		int tempSize = temp1.size();
	      for(int i = 0; i < tempSize; i++) {
	     	 KeyClass key = new StringKey(temp1.get(i).toString());
					
					  if(i < tempSize-3) {
						  String dupli1 = temp1.get(i).toString().substring(0, temp1.get(i).toString().indexOf(":", temp1.get(i).toString().indexOf(":") + 1));
						  String dupli2 = temp1.get(i+1).toString().substring(0, temp1.get(i+1).toString().indexOf(":", temp1.get(i+1).toString().indexOf(":")+ 1)); 
						  String dupli3 = temp1.get(i+2).toString().substring(0, temp1.get(i+2).toString().indexOf(":", temp1.get(i+2).toString().indexOf(":") + 1)); 
						  String dupli4 = temp1.get(i+3).toString().substring(0, temp1.get(i+3).toString().indexOf(":", temp1.get(i+3).toString().indexOf(":")+ 1)); 
						  int t1 = dupli1.compareTo(dupli2); 
						  int t2 = dupli1.compareTo(dupli3);
						  int t3 = dupli1.compareTo(dupli4);
						  if((t1 == 0) && (t2 == 0) && (t3 == 0)) {
						  }
						  else { 
							  Map_BTreeIndex_Subinsert.insert(key,mid); 
						  }
					  } 
					  else { 
						  Map_BTreeIndex_Subinsert.insert(key,mid); 
					  }
	      }
	      
	      BTFileScan scan1 = Map_BTreeIndex_Subinsert.new_scan(null,null);
	      
	      ArrayList<String> allData = new ArrayList<String>();
	      
			while((entry = scan1.get_next())!= null)
				{					
				allData.add(entry.key.toString()); 
				}
			scan1.DestroyBTreeFileScan();
			
			int allDataSize = allData.size();
			for(int i =0 ; i < allDataSize; i++) {
				String item = allData.get(i);
				int index = item.length()-1;
				char type = item.charAt(index);
				String[] strs = item.split(":");
				if(type == '1') {
					String mapStr = (strs[2]  + ":" + strs[0] + ":" +  strs[1]+  ":" + strs[3]);
					KeyClass key = new StringKey(mapStr);
					Map_BTreeIndex_StorageIndex1.insert(key, mid);
				}
				if(type == '2') {
					String mapStr = (strs[0] + ":" + strs[2]  + ":" + strs[1]+  ":" + strs[3]);
					KeyClass key = new StringKey(mapStr);
					Map_BTreeIndex_StorageIndex2.insert(key, mid);
				}
				if(type == '3') {
					String mapStr = (strs[1] + ":" + strs[2]  + ":" + strs[0]+  ":" + strs[3]);
					KeyClass key = new StringKey(mapStr);
					Map_BTreeIndex_StorageIndex3.insert(key, mid);
				}
				if(type == '4') {
					String mapStr = (strs[1] + ":" + strs[0]+  ":" + strs[2]  + ":" +  strs[3]);
					KeyClass key = new StringKey(mapStr);
					Map_BTreeIndex_StorageIndex4.insert(key, mid);
				}
				if(type == '5') {
					String mapStr = (strs[0] + ":" + Util.padLeftZeros(strs[3],25)+  ":" + strs[2]  + ":" +  strs[1]); //prakash added padding to strs[3]
					KeyClass key = new StringKey(mapStr);
					Map_BTreeIndex_StorageIndex5.insert(key, mid);
				}
			}
			BTFileScan tscan = Map_BTreeIndex_StorageIndex1.new_scan(null, null);
			BTreeFile Map_MTree2,Map_MTree3,Map_MTree4,Map_MTree5;
			MID temp;
			KeyClass treeKey=null;
	      if (true) {
	     	
	     	KeyDataEntry result;
			while((result = tscan.get_next())!=null) {
				String item = result.key.toString();

			 	String[] strs = item.split(":");
			 	
			 try {
				 map=new Map();
				map.setRowLabel(strs[1]);
				map.setColumnLabel(strs[2]);
				map.setTimeStamp(Integer.parseInt(strs[0]));
				map.setValue(strs[3]);					
				bhf_1.insertMap(map.getMapByteArray());
			}
			 catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	     	}
	          tscan.DestroyBTreeFileScan();
	  ////////////////////////////////////////////////////////////////////////////////////////        
			tscan = Map_BTreeIndex_StorageIndex2.new_scan(null, null);
			Map_MTree2 = new BTreeFile(bhf_2._fileName+"/Map_BTreeIndex",AttrType.attrString,25,1);
			Map_MTree2.destroyFile();
			Map_MTree2 = new BTreeFile(bhf_2._fileName+"/Map_BTreeIndex",AttrType.attrString,25,1);
			while((result = tscan.get_next())!=null) {
				String item = result.key.toString();

			 	String[] strs = item.split(":");
			 	
			 try {
				 map=new Map();
				map.setRowLabel(strs[0]);
				map.setColumnLabel(strs[2]);
				map.setTimeStamp(Integer.parseInt(strs[1]));
				map.setValue(strs[3]);					
				temp=bhf_2.insertMap(map.getMapByteArray());
				treeKey=new StringKey(strs[0]);
				Map_MTree2.insert(treeKey, temp);
			}
			 catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	     	}
			Map_MTree2.close();
			tscan.DestroyBTreeFileScan();
			
////////////////////////////////////////////////////////////////////////////////////////////			
			
			
			tscan = Map_BTreeIndex_StorageIndex3.new_scan(null, null);
			Map_MTree3 = new BTreeFile(bhf_3._fileName+"/Map_BTreeIndex",AttrType.attrString,25,1);
			Map_MTree3.destroyFile();
			Map_MTree3 = new BTreeFile(bhf_3._fileName+"/Map_BTreeIndex",AttrType.attrString,25,1);
			while((result = tscan.get_next())!=null) {
				String item = result.key.toString();

			 	String[] strs = item.split(":");
			 	
			 try {
				 map=new Map();
				map.setRowLabel(strs[2]);
				map.setColumnLabel(strs[0]);
				map.setTimeStamp(Integer.parseInt(strs[1]));
				map.setValue(strs[3]);					
				temp=bhf_3.insertMap(map.getMapByteArray());
				treeKey=new StringKey(strs[0]);
				Map_MTree3.insert(treeKey, temp);
			}
			 catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	     	}
			Map_MTree3.close();
			tscan.DestroyBTreeFileScan();

	////////////////////////////////////////////////////////////////////////////////////////////
			
			tscan = Map_BTreeIndex_StorageIndex4.new_scan(null, null);
			
			Map_MTree4 = new BTreeFile(bhf_4._fileName+"/Map_BTreeIndex",AttrType.attrString,56,1);
			Map_MTree4.destroyFile();
			Map_MTree4 = new BTreeFile(bhf_4._fileName+"/Map_BTreeIndex",AttrType.attrString,56,1);
			
			while((result = tscan.get_next())!=null) {
				String item = result.key.toString();

			 	String[] strs = item.split(":");
			 	
			 try {
				 map=new Map();
				map.setRowLabel(strs[1]);
				map.setColumnLabel(strs[0]);
				map.setTimeStamp(Integer.parseInt(strs[2]));
				map.setValue(strs[3]);					
				temp=bhf_4.insertMap(map.getMapByteArray());
				treeKey=new StringKey(Util.padRightZeros(strs[0],25)+":"+Util.padRightZeros(strs[1],25));
				Map_MTree4.insert(treeKey, temp);
			}
			 catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	     	}
			Map_MTree4.close();
			tscan.DestroyBTreeFileScan();
			
	////////////////////////////////////////////////////////////////////////////////////////////
			
			tscan = Map_BTreeIndex_StorageIndex5.new_scan(null, null);
			
			Map_MTree5 = new BTreeFile(bhf_5._fileName+"/Map_BTreeIndex",AttrType.attrString,56,1);
			Map_MTree5.destroyFile();
			Map_MTree5 = new BTreeFile(bhf_5._fileName+"/Map_BTreeIndex",AttrType.attrString,56,1);
			
			while((result = tscan.get_next())!=null) {
				String item = result.key.toString();

			 	String[] strs = item.split(":");
			 	
			 try {
				 map=new Map();
				map.setRowLabel(strs[0]);
				map.setColumnLabel(strs[3]);
				map.setTimeStamp(Integer.parseInt(strs[2]));
				map.setValue(Integer.parseInt(strs[1])+"");//prakash changed					
				temp=bhf_5.insertMap(map.getMapByteArray());
				treeKey=new StringKey(Util.padRightZeros(strs[0], 25)+":"+strs[1]);
				Map_MTree5.insert(treeKey, temp);
			}
			 catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
	     	}
			Map_MTree5.close();
			tscan.DestroyBTreeFileScan();
	      }
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		Map_BTreeIndex_insert.destroyFile();
		Map_BTreeIndex_insert.close();
		Map_BTreeIndex_Subinsert.destroyFile();
		Map_BTreeIndex_Subinsert.close();
		Map_BTreeIndex_StorageIndex1.destroyFile();
		Map_BTreeIndex_StorageIndex1.close();
		Map_BTreeIndex_StorageIndex2.destroyFile();
		Map_BTreeIndex_StorageIndex2.close();
		Map_BTreeIndex_StorageIndex3.destroyFile();
		Map_BTreeIndex_StorageIndex3.close();
		Map_BTreeIndex_StorageIndex4.destroyFile();
		Map_BTreeIndex_StorageIndex4.close();
		Map_BTreeIndex_StorageIndex5.destroyFile();
		Map_BTreeIndex_StorageIndex5.close();

		if (bhf_1.getMapCnt() == 0)
			bhf_1.deleteBigt();
		else
			System.out.println("Total Maps Inserted in type 1 : " + bhf_1.getMapCnt());
		if (bhf_2.getMapCnt() == 0) {
			bhf_2.deleteBigt();
			Map_MTree2.destroyFile();
			Map_MTree2.close();
		}
		else
			System.out.println("Total Maps Inserted in type 2 : " + bhf_2.getMapCnt());
		if (bhf_3.getMapCnt() == 0) {
			bhf_3.deleteBigt();
			Map_MTree3.destroyFile();
			Map_MTree3.close();
		}
		else
			System.out.println("Total Maps Inserted in type 3 : " + bhf_3.getMapCnt());
		if (bhf_4.getMapCnt() == 0) {
			bhf_4.deleteBigt();
			Map_MTree4.destroyFile();
			Map_MTree4.close();
		}
		else
			System.out.println("Total Maps Inserted in type 4 : " + bhf_4.getMapCnt());
		if (bhf_5.getMapCnt() == 0) {
			bhf_5.deleteBigt();
			Map_MTree5.destroyFile();
			Map_MTree5.close();
		}
		else
			System.out.println("Total Maps Inserted in type 5 : " + bhf_5.getMapCnt());

		if (status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {
			System.err.println("* There are left pages pinned\n");
			status = FAIL;
		}

		if (status == OK) {
			System.out.println("Batch nodes insertion completed successfully.\n");
		}

	}

	public static void mergeBigT()
			throws InvalidSlotNumberException, InvalidMapSizeException, FileAlreadyDeletedException, Exception {

		PCounter.initialize();
		BigTDBManager bdb = new BigTDBManager();
		bdb.init("BigDB", 1000);

		Page apage = new Page();
		boolean found = false;
		int slot = 0;
		PageId hpid = new PageId();
		PageId nexthpid = new PageId(0);
		DBHeaderPage dp;
		
		HashMap kvPair = new HashMap<String, Integer>();
		// System.out.println("get_file_entry do-loop01: "+name);
		
		do {
		hpid.pid = nexthpid.pid;

		// Pin the header page.
		// pinPage(hpid, apage, false /*no diskIO*/);
		SystemDefs.JavabaseBM.pinPage(hpid, apage, false);
		// This complication is because the first page has a different
		// structure from that of subsequent pages.
		if (hpid.pid == 0) {
			dp = new DBFirstPage();
			((DBFirstPage) dp).openPage(apage);
		} else {
			dp = new DBDirectoryPage();
			((DBDirectoryPage) dp).openPage(apage);
		}
		nexthpid = dp.getNextPage();

		int entry = 0;
		PageId tmppid = new PageId();
		String tmpname;

		 
		while (entry < dp.getNumOfEntries()) {

			tmpname = dp.getFileEntry(tmppid, entry);
			//System.out.println(tmpname + " " + entry);
			if (tmpname.length() > 1 && !tmpname.contains("/")) {
				String[] name = tmpname.split("_");

				 
				if (kvPair.containsKey(name[0])) {
					bigT file1 = new bigT(tmpname);

					Integer count = (Integer) kvPair.get(name[0]);
					kvPair.put(name[0], count + file1.getMapCnt());
				} else {
					bigT file1 = new bigT(tmpname);

					kvPair.put(name[0], file1.getMapCnt());
				}
//		    bigT file1 = new bigT(tmpname);
//		    String[] name = tmpname.split("-");
//		    temp.add(name[0]+"-" + file1.getMapCnt());

			}
			entry++;
		}
		SystemDefs.JavabaseBM.unpinPage(hpid, false /* undirty */);
		}
		while(nexthpid.pid != INVALID_PAGE);
		

		// int size = temp.size();
		Iterator hmIterator = kvPair.entrySet().iterator();
		while (hmIterator.hasNext()) {
			java.util.Map.Entry mapElement = (java.util.Map.Entry) hmIterator.next();
			System.out.println(mapElement.getKey() + " table has " + mapElement.getValue() + " maps");

			bigT mergedBigT = getMergedBigT(mapElement.getKey().toString());
			getDistinctRowLabelCount(mergedBigT);
			getDistinctColumnLabelCount(mergedBigT);
			mergedBigT.deleteBigt();
			System.out.println();
		}
		bdb.close();
		Util.printStatInfo();
	}

	private static void getDistinctColumnLabelCount(bigT mergedBigT)
			throws InvalidTypeException, PageNotReadException, BUtilsException, BSortException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, IOException, Exception {
		BIterator sort = Util.createSortIteratorForMap(mergedBigT, 2);

		int columnLabelCount = 0;
		Map tuple = null;
		String currentColumnLabel = "", previousColumnLabel = "";

		try {
			tuple = sort.get_next();
			previousColumnLabel = tuple.getColumnLabel();
		} catch (Exception e) {
			e.printStackTrace();
		}
		while ((tuple = sort.get_next()) != null) {
			try {
				currentColumnLabel = tuple.getColumnLabel();
				if (previousColumnLabel.compareTo(currentColumnLabel) != 0) {
					columnLabelCount++;
				}
				previousColumnLabel = currentColumnLabel;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		sort.close();
		System.out.println(mergedBigT._fileName + " table has " + ++columnLabelCount + " Distinct Column labels");

	}

	private static void getDistinctRowLabelCount(bigT mergedBigT)
			throws InvalidTypeException, PageNotReadException, BUtilsException, BSortException, LowMemException,
			UnknowAttrType, UnknownKeyTypeException, IOException, Exception {

		BIterator sort = Util.createSortIteratorForMap(mergedBigT, 3);

		int rowLabel = 0;
		Map tuple = null;
		String currentRowLabel = "", previousRowLabel = "";

		try {
			tuple = sort.get_next();
			previousRowLabel = tuple.getRowLabel();
		} catch (Exception e) {
			e.printStackTrace();
		}
		while ((tuple = sort.get_next()) != null) {
			try {
				currentRowLabel = tuple.getRowLabel();
				if (previousRowLabel.compareTo(currentRowLabel) != 0) {
					rowLabel++;
				}
				previousRowLabel = currentRowLabel;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		sort.close();
		System.out.println(mergedBigT._fileName + " table has " + ++rowLabel + " Distinct Row labels");

	}

	private static bigT getMergedBigT(String value) throws InvalidSlotNumberException, InvalidMapSizeException,
			HFDiskMgrException, HFBufMgrException, FileAlreadyDeletedException, IOException, Exception {
		bigT mergedHF = null;
		try {
			bhf_1 = new bigT(value + "_1");
			bhf_2 = new bigT(value + "_2");
			bhf_3 = new bigT(value + "_3");
			bhf_4 = new bigT(value + "_4");
			bhf_5 = new bigT(value + "_5");
			mergedHF = new bigT(value);
			mergedHF.deleteBigt();
			mergedHF = new bigT(value);
		} catch (Exception e) {
			e.printStackTrace();
		}
		MID mid = new MID();
		Map map = new Map();

		if (bhf_1.getMapCnt() > 0) {
			BScan bscan = bhf_1.openScan();
			while ((map = bscan.getNext(mid)) != null) {
				mergedHF.insertMap(map.getMapByteArray());
			}
			bscan.closescan();
		} else
			bhf_1.deleteBigt();
		if (bhf_2.getMapCnt() > 0) {
			BScan bscan = bhf_2.openScan();
			while ((map = bscan.getNext(mid)) != null) {
				mergedHF.insertMap(map.getMapByteArray());
			}
			bscan.closescan();
		} else
			bhf_2.deleteBigt();
		if (bhf_3.getMapCnt() > 0) {
			BScan bscan = bhf_3.openScan();
			while ((map = bscan.getNext(mid)) != null) {
				mergedHF.insertMap(map.getMapByteArray());
			}
			bscan.closescan();
		} else
			bhf_3.deleteBigt();
		if (bhf_4.getMapCnt() > 0) {
			BScan bscan = bhf_4.openScan();
			while ((map = bscan.getNext(mid)) != null) {
				mergedHF.insertMap(map.getMapByteArray());
			}
			bscan.closescan();
		} else
			bhf_4.deleteBigt();
		if (bhf_5.getMapCnt() > 0) {
			BScan bscan = bhf_5.openScan();
			while ((map = bscan.getNext(mid)) != null) {
				mergedHF.insertMap(map.getMapByteArray());
			}
			bscan.closescan();
		} else
			bhf_5.deleteBigt();

		return mergedHF;
	}

	public static void runMapInsert(String[] arguments)
			throws IOException, FileIOException, InvalidPageNumberException, DiskMgrException,
			InvalidSlotNumberException, InvalidMapSizeException, HFDiskMgrException, HFBufMgrException,
			KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException,
			ConstructPageException, UnpinPageException, PinPageException, NodeNotMatchException, ConvertException,
			DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException, InsertException,
			ScanIteratorException, PageUnpinnedException, InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException, FreePageException, DeleteFileEntryException, FileAlreadyDeletedException, HFException,
			GetFileEntryException, AddFileEntryException {
		// mapinsert RL CL VAL TS TYPE BIGTABLENAME

		BigTDBManager bdb = new BigTDBManager();
		bdb.init("BigDB", 1000);

		Map map = new Map();
		map.setRowLabel(arguments[1]);
		map.setColumnLabel(arguments[2]);
		map.setValue(arguments[3]);
		map.setTimeStamp(Integer.parseInt(arguments[4]));
		Integer type = Integer.parseInt(arguments[5]);
		String bigTname = arguments[6];

		createbigTHeapFile(type, bigTname, map);

		bdb.close();
	}

}// end of BigTDB class