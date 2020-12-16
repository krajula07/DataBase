package BigTtests;

import BigT.bigT;
import BigTIterator.BSort;
import BigT.BScan;
import diskmgr.PCounter;
import global.*;
import iterator.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import BDescIterator.BDSort;

public class Util {
	/**
	 * read data from test data file
	 * 
	 * @param datafile
	 * @return
	 */
	public static ArrayList<String> readDataFromFile(File datafile) {

		ArrayList<String> data = new ArrayList<>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(datafile));
			String line = null;
			while ((line = reader.readLine()) != null) {
				data.add(line);
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return data;
	}

	public static void printStatInfo() {
		try {

			System.out.println("Number of disk pages that were read: " + PCounter.rcounter);
			System.out.println("Number of disk pages that were written: " + PCounter.wcounter);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * create a sort iterator for map, sorted by map labels
	 *
	 * @param mapHeapFileName
	 * @return
	 */
	public static BSort createSortIteratorForMap(bigT nodeHeapFileName, int order_map) {

		AttrType[] attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrString);
		attrType[3] = new AttrType(AttrType.attrString);
		attrType[2] = new AttrType(AttrType.attrInteger);

		short[] attrSize = new short[4];
		attrSize[0] = 22;
		attrSize[1] = 18;
		attrSize[2] = 4;
		attrSize[3] = 10;

		FldSpec[] projList = new FldSpec[4]; // output - node format
		RelSpec rel = new RelSpec(RelSpec.outer);
		projList[0] = new FldSpec(rel, 1);
		projList[1] = new FldSpec(rel, 2);
		projList[2] = new FldSpec(rel, 3);
		projList[3] = new FldSpec(rel, 4);

		// int orderMap=order_map;
		MapOrder[] orderMap = new MapOrder[1];
		switch (order_map) {
		case 1:
			orderMap[0] = new MapOrder(MapOrder.RowColumnTimestamp);
			break;
		case 2:
			orderMap[0] = new MapOrder(MapOrder.ColumnRowTimestamp);
			break;
		case 3:
			orderMap[0] = new MapOrder(MapOrder.RowTimestamp);
			break;
		case 4:
			orderMap[0] = new MapOrder(MapOrder.ColumnTimestamp);
			break;
		case 5:
			orderMap[0] = new MapOrder(MapOrder.Timestamp);
			break;
		case 6:
			orderMap[0] = new MapOrder(MapOrder.Value);
			break;
		case 7:
			orderMap[0] = new MapOrder(MapOrder.Descending);
			break;
		case 8:
			orderMap[0] = new MapOrder(MapOrder.Ascending);
			break;
		case 9:
			orderMap[0] = new MapOrder(MapOrder.HmapTrail);
			break;
		default:
			System.out.println("Invalid order");
		}
		// create file scan on node heap file
		BScan fscan = null;
		try {

			fscan = new BScan(nodeHeapFileName);

		} catch (Exception e) {
			e.printStackTrace();
		}

		// create sort iterator for node
		BSort sort = null;
		int sortField = 1; // sort on node label
		int sortFieldLength = 10;
		int SORTPGNUM = 30;
		try {
			// System.out.println("before sort.............");
			sort = new BSort(attrType, (short) attrType.length, attrSize, fscan, sortField, orderMap[0],
					sortFieldLength, SORTPGNUM);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sort;
	}
public static BDSort createSortIteratorDescForMap(bigT nodeHeapFileName,int order_map) {
    	
    	AttrType[] attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[3] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrInteger);
        
        short[] attrSize = new short[4];
        attrSize[0] = 50;
        attrSize[1] = 25;
        attrSize[2] = 4;
        attrSize[3] = 25;
        
        FldSpec[] projList = new FldSpec[4]; //output - node format
        RelSpec rel = new RelSpec(RelSpec.outer);
        projList[0] = new FldSpec(rel, 1);
        projList[1] = new FldSpec(rel, 2);
        projList[2] = new FldSpec(rel, 3);
        projList[3] = new FldSpec(rel, 4);
        
       // int orderMap=order_map;
        MapOrder[] orderMap = new MapOrder[1];
switch(order_map){
case 1 :
	orderMap[0]=new MapOrder(MapOrder.RowColumnTimestamp);
    break;
 case 2 :
	 orderMap[0] = new MapOrder(MapOrder.ColumnRowTimestamp);
	 break;
 case 3 :
	 orderMap[0] = new MapOrder(MapOrder.RowTimestamp);
    break;
 case 4 :
	 orderMap[0] = new MapOrder(MapOrder.ColumnTimestamp);
	 break;
 case 5 :
	 orderMap[0] = new MapOrder(MapOrder.Timestamp);
    break;
 case 6 :
	 orderMap[0] = new MapOrder(MapOrder.Value);
	    break;
 case 7 :
	 orderMap[0] = new MapOrder(MapOrder.Descending);
	    break;
 case 8:
	 orderMap[0] = new MapOrder(MapOrder.Ascending);
	    break;
 
 default :
    System.out.println("Invalid order");
}
       /* MapOrder[] order = new MapOrder[8];
        order[0] = new MapOrder(MapOrder.Ascending);
        order[1] = new MapOrder(MapOrder.Descending);
        order[2] = new MapOrder(MapOrder.Ascending);
        
        order[3] = new MapOrder(MapOrder.RowColumnTimestamp);
        order[4] = new MapOrder(MapOrder.ColumnRowTimestamp);
        order[5] = new MapOrder(MapOrder.RowTimestamp);
        order[6] = new MapOrder(MapOrder.ColumnTimestamp);
        order[7] = new MapOrder(MapOrder.Timestamp);*/
        

        // create file scan on node heap file
        BScan fscan = null;
        try {
        	
            fscan = new BScan(nodeHeapFileName);
            
        } catch (Exception e) {
            e.printStackTrace();
        }

	 BDSort sort = null;
     int sortField = 1; // sort on node label
     int sortFieldLength = 50;
     int SORTPGNUM = 30;
     try {
     	//System.out.println("before sort.............");
         sort = new BDSort(attrType, (short) attrType.length, attrSize, fscan, sortField, orderMap[0], sortFieldLength, SORTPGNUM);
     } catch (Exception e) {
         e.printStackTrace();
     }
   //  System.out.println("kavya in util");
     return sort;
	

    } 

	public static BSort createSortValueIteratorForMap(bigT nodeHeapFileName) {

		AttrType[] attrType = new AttrType[4];
		attrType[0] = new AttrType(AttrType.attrString);
		attrType[1] = new AttrType(AttrType.attrString);
		attrType[3] = new AttrType(AttrType.attrString);
		attrType[2] = new AttrType(AttrType.attrInteger);

		short[] attrSize = new short[4];
		attrSize[0] = 25;
		attrSize[1] = 25;
		attrSize[2] = 4;
		attrSize[3] = 25;

		FldSpec[] projList = new FldSpec[4]; // output - node format
		RelSpec rel = new RelSpec(RelSpec.outer);
		projList[0] = new FldSpec(rel, 1);
		projList[1] = new FldSpec(rel, 2);
		projList[2] = new FldSpec(rel, 3);
		projList[3] = new FldSpec(rel, 4);

		MapOrder[] order = new MapOrder[9];
		order[0] = new MapOrder(MapOrder.Ascending);
		order[1] = new MapOrder(MapOrder.Descending);
		order[2] = new MapOrder(MapOrder.Ascending);

		order[3] = new MapOrder(MapOrder.RowColumnTimestamp);
		order[4] = new MapOrder(MapOrder.ColumnRowTimestamp);
		order[5] = new MapOrder(MapOrder.RowTimestamp);
		order[6] = new MapOrder(MapOrder.ColumnTimestamp);
		order[7] = new MapOrder(MapOrder.Timestamp);
		order[8] = new MapOrder(MapOrder.Value);

		// create file scan on node heap file
		BScan fscan = null;
		try {

			fscan = new BScan(nodeHeapFileName);

		} catch (Exception e) {
			e.printStackTrace();
		}

		// create sort iterator for node
		BSort sort = null;
		int sortField = 4; // sort on node label
		int sortFieldLength = 25;
		int SORTPGNUM = 30;
		try {
			// System.out.println("before sort.............");
			sort = new BSort(attrType, (short) attrType.length, attrSize, fscan, sortField, order[8], sortFieldLength,
					SORTPGNUM);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sort;
	}

	public static String padRightZeros(String inputString, int length) {

		while (inputString.length() < length) {
			inputString = inputString + '0';
		}

		return inputString;
	}

	public static String padLeftZeros(String inputString, int length) {

		StringBuilder sb = new StringBuilder();
		while (sb.length() < length - inputString.length()) {
			sb.append('0');
		}
		sb.append(inputString);

		return sb.toString();
	}

	public static String padLeftZeros(int input, int length) {
		String inputString = input + "";
		StringBuilder sb = new StringBuilder();
		while (sb.length() < length - inputString.length()) {
			sb.append('0');
		}
		sb.append(inputString);

		return sb.toString();
	}

}