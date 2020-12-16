package BigT;

import BigTIterator.BIterator;
import BigTtests.Util;
import iterator.Sort;
import btree.*;
import global.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.InvalidSlotNumberException;
import heap.Scan;

public class Stream {

	public static SystemDefs sysdef = null;
	public static String dbName;
	public static int Sortoption = 1; // Index option
	public static bigT Result_HF = null;
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
	public String ColumnLabelFilter;
	public String ValueFilter;
	public int TimeStampFilter;
	public boolean scan_on_BT = false;
	public Map scan_on_BT_map = null;

	// Constructor
	public Stream(String bigTableName, int orderType, String rowFilter, String columnFilter, String valueFilter)
			throws Exception {
		Sortoption = orderType;
		// String BigTdbname=bigHeapFile._fileName;
		RowLabel_null = false;
		ColumnLabel_null = false;
		Value_null = false;

		java.util.Date date = new java.util.Date();
		Result_HF = new bigT(Long.toString(date.getTime()));

		if (rowFilter.compareToIgnoreCase("null") == 0 || rowFilter.compareToIgnoreCase("*") == 0
				|| rowFilter.compareToIgnoreCase("[null,null]") == 0) {
			RowLabel_null = true;
		}
		if (columnFilter.compareToIgnoreCase("null") == 0 || columnFilter.compareToIgnoreCase("*") == 0
				|| columnFilter.compareToIgnoreCase("[null,null]") == 0) {
			ColumnLabel_null = true;
		}
		if (valueFilter.compareToIgnoreCase("null") == 0 || valueFilter.compareToIgnoreCase("*") == 0
				|| valueFilter.compareToIgnoreCase("[null,null]") == 0) {
			Value_null = true;
		}

		int i = 1;

		bigT bigHeapFile = null;

		while (i <= 5) {
			// Fetching the index option
			bigHeapFile = new bigT(bigTableName + "_" + i);

			if (bigHeapFile.getMapCnt() > 0) {

				if (i == 2 && !RowLabel_null) {
					//System.out.println("lol");
					streamByRowIndex(bigHeapFile, rowFilter, columnFilter, valueFilter);
				} else if (i == 3 && !ColumnLabel_null) {
					//System.out.println("lol");
					streamByColumnIndex(bigHeapFile, rowFilter, columnFilter, valueFilter);
				} else if (i == 4 && !ColumnLabel_null && !RowLabel_null) {
					streamByColumnRow(bigHeapFile, rowFilter, columnFilter, valueFilter);
				} else if (i == 5 && !RowLabel_null && !Value_null) {
					streamByRowValue(bigHeapFile, rowFilter, columnFilter, valueFilter);
				}

				else {
					//System.out.println("lol");
					ScanEntirebigT(bigHeapFile, rowFilter, columnFilter, valueFilter);
				}
			}
			else
				bigHeapFile.deleteBigt();

			i++;
		}
		i = 1;
		System.out.println("Filtered Map Count:- " + Result_HF.getMapCnt());
		
//		BScan b1=new BScan(Result_HF);
//		Map map1=null;
//		MID mid1= new MID();
//		while ((map1 = (Map) b1.getNext(mid1)) != null) {
//			System.out.println(map1.getRowLabel() + "," + map1.getColumnLabel() + "," + map1.getTimeStamp() + ","
//					+ map1.getValue());
//		}

		if (Result_HF.getMapCnt() > 0) {
			int keytype = AttrType.attrString;
			BScan am = new BScan(Result_HF);
			Map map = null;

			MID mid = new MID();

			MID mapid = null;

			Map record = null;

			KeyDataEntry entry1 = null;

			BTFileScan scan = null;

			while ((map = (Map) am.getNext(mid)) != null) {
			}

			BIterator sort = Util.createSortIteratorForMap(Result_HF, orderType);
			Map tuple = null;

			try {
				tuple = sort.get_next();
				System.out.println(tuple.getRowLabel() + "," + tuple.getColumnLabel() + "," + tuple.getTimeStamp() + ","
						+ tuple.getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
			while ((tuple = sort.get_next()) != null) {

				try {
					System.out.println(tuple.getRowLabel() + "," + tuple.getColumnLabel() + "," + (tuple.getTimeStamp())
							+ "," + tuple.getValue());
				}

				catch (Exception e) {
					e.printStackTrace();
				}
			}
			System.out.println("Filtered Map Count:- " + Result_HF.getMapCnt());
			am.closescan();
			sort.close();
			
		}

	}

	// If the Index option selected is on Row Label and Row Label filter is not NULL
	private void streamByRowIndex(bigT bigHeapFile, String rowFilter, String columnFilter, String valueFilter)
			throws Exception {
		boolean result = true;
		KeyDataEntry entry1 = null;
		MID mid = null;
		String rowlabel = null, columnlabel = null, value = null;
		Map rMap = null; // result Map

		bigT bhf = bigHeapFile;

		int keytype = AttrType.attrString;
		BTreeFile Map_MTree = new BTreeFile(bigHeapFile._fileName + "/Map_BTreeIndex", keytype, 25, 1);

		KeyClass low_key = null;
		KeyClass high_key = null;

		// Row Label Filter: Range Search
		if (rowFilter.startsWith("[")) {
			StringBuilder str = new StringBuilder(rowFilter);
			String[] strarray = ((str.deleteCharAt(str.length() - 1)).deleteCharAt(0)).toString().split(",");
			if (!strarray[0].equalsIgnoreCase("null"))
				low_key = new StringKey(strarray[0]);
			if (!strarray[1].equalsIgnoreCase("null"))
				high_key = new StringKey(strarray[1]);
		}

		// Row Label Filter: Equality Search
		else {
			low_key = new StringKey(rowFilter);
			high_key = new StringKey(rowFilter);
		}

		BTFileScan scan = Map_MTree.new_scan(low_key, high_key); // Creating a B Tree file scan object

		while ((entry1 = scan.get_next()) != null) {
			result = true;
			mid = ((LeafData) entry1.data).getData();
			rMap = bhf.getRecord(mid);
			rowlabel = rMap.getRowLabel();
			columnlabel = rMap.getColumnLabel();
			value = rMap.getValue();

			// Column Label Filer
			if (!ColumnLabel_null && result) {
				result = columnLabelCheck( columnFilter , columnlabel);
			}

			int valueInInt = Integer.parseInt(value);

			// Value filter
			if (!Value_null && result) {
				result = valueLabelCheck(valueFilter, valueInInt);
			}

			if (result) {
				Result_HF.insertMap(rMap.getMapByteArray()); // Inserting the filtered Maps into the Result_HF file
			}
		}
		scan.DestroyBTreeFileScan();
		Map_MTree.close();
	}

	// If the Index option selected is on Column Label and Column Label filter is
	// not NULL
	private void streamByColumnIndex(bigT bigHeapFile, String rowFilter, String columnFilter, String valueFilter)
			throws Exception {
		boolean result = true;
		KeyDataEntry entry1 = null;
		MID mapid = null;
		String rowlabel = null, columnlabel = null, value = null;
		Map record = null;
		bigT Map_HF = bigHeapFile;

		int keytype = AttrType.attrString;
		BTreeFile Map_MTree = new BTreeFile(bigHeapFile._fileName + "/Map_BTreeIndex", keytype, 25, 1);

		KeyClass low_key = null;
		KeyClass high_key = null;

		// Column Label: Range Search
		if (columnFilter.startsWith("[")) {
			StringBuilder str = new StringBuilder(columnFilter);
			String[] strarray = ((str.deleteCharAt(str.length() - 1)).deleteCharAt(0)).toString().split(",");
			if (!strarray[0].equalsIgnoreCase("null"))
				low_key = new StringKey(strarray[0]);
			if (!strarray[1].equalsIgnoreCase("null"))
				high_key = new StringKey(strarray[1]);
		}

		// Column Label: Equality Search
		else {
			low_key = new StringKey(columnFilter);
			high_key = new StringKey(columnFilter);
		}

		BTFileScan scan = Map_MTree.new_scan(low_key, high_key);

		while ((entry1 = scan.get_next()) != null) {
			result = true;

			mapid = ((LeafData) entry1.data).getData();
			record = (Map) Map_HF.getRecord(mapid);
			rowlabel = record.getRowLabel();
			columnlabel = record.getColumnLabel();
			value = record.getValue();

			// Row Label filter
			if (!RowLabel_null && result) {
				result = rowLabelCheck(rowFilter, rowlabel);
			}
			/*if (!ColumnLabel_null && result) {
				result = rowLabelCheck(columnFilter, columnlabel);
			}*/

			int valueInInt = Integer.parseInt(value);

			// Value filter
			if (!Value_null && result) {
				result = valueLabelCheck(valueFilter, valueInInt);
			}

			if (result) {
				Result_HF.insertMap(record.getMapByteArray()); // Inserting the filtered Maps into the Result_HF file
			}
		}
		scan.DestroyBTreeFileScan();
		Map_MTree.close();
	}

	private void streamByRowValue(bigT bigHeapFile, String rowFilter, String columnFilter, String valueFilter)
			throws Exception {
		boolean result = true;
		KeyDataEntry entry1 = null;
		MID mapid = null;
		String rowlabel = null, columnlabel = null, value = null;
		Map record = null;

		bigT Map_HF = bigHeapFile;

		int keytype = AttrType.attrString;
		BTreeFile Map_MTree = new BTreeFile(bigHeapFile._fileName + "/Map_BTreeIndex", keytype, 56, 1);

		int integerType = AttrType.attrInteger;
		BTreeFile Map_MTree1 = new BTreeFile(bigHeapFile._fileName + "/Map_BTreeIndex1", integerType, 4, 1);

		KeyClass low_key = null;
		KeyClass high_key = null;

		String low_key1 = null;
		String high_key1 = null;

		String low_key2 = null;
		String high_key2 = null;

		Boolean isString1 = false;
		Boolean isString2 = false;

		if (rowFilter.startsWith("[")) {
			StringBuilder str = new StringBuilder(rowFilter);
			String[] strarray = ((str.deleteCharAt(str.length() - 1)).deleteCharAt(0)).toString().split(",");
			if (!strarray[0].equalsIgnoreCase("null"))
				low_key1 = strarray[0];
			if (!strarray[1].equalsIgnoreCase("null"))
				high_key1 = strarray[1];
		}

		else {
			isString1 = true;
		}

		if (valueFilter.startsWith("[")) {
			StringBuilder str = new StringBuilder(valueFilter);
			String[] strarray = ((str.deleteCharAt(str.length() - 1)).deleteCharAt(0)).toString().split(",");
			if (!strarray[0].equalsIgnoreCase("null"))
				low_key2 = strarray[0];
			if (!strarray[1].equalsIgnoreCase("null"))
				high_key2 = strarray[1];
		}

		else {
			isString2 = true;
		}

		if (isString1) {// if row is equality search
			if (isString2) {// if value is equality search
				// both are equality searches
				valueFilter = Util.padLeftZeros(valueFilter, 25);
				String rowString = Util.padRightZeros(rowFilter, 25);

				low_key = new StringKey(rowString + ":" + valueFilter);
				high_key = new StringKey(rowString + ":" + valueFilter);

			} else {
				// row is equality search and value is range range search
				if (low_key2 == null) {// In value range search if low key is null ([null,string2])
					low_key = new StringKey(Util.padRightZeros(rowFilter, 25) + ":");// to fetch all the maps start with
																						// rowfilter: and greater than
																						// low key
					high_key = new StringKey(
							Util.padRightZeros(rowFilter, 25) + ":" + Util.padLeftZeros(high_key2, 25));
				} else if (high_key2 == null) {// In value range search if high key is null([string1,null])
					low_key = new StringKey(Util.padRightZeros(rowFilter, 25) + ":" + Util.padLeftZeros(low_key2, 25));
					high_key = new StringKey(Util.padRightZeros(rowFilter, 25) + ":~");// to fetch all the maps start
																						// with valuefilter: and less
																						// than high key
				} else {
					// if value range search is like [string1,string2]
					low_key = new StringKey(Util.padRightZeros(rowFilter, 25) + ":" + Util.padLeftZeros(low_key2, 25));
					high_key = new StringKey(
							Util.padRightZeros(rowFilter, 25) + ":" + Util.padLeftZeros(high_key2, 25));
				}
			}
		} else {
			// if row is range search
			if (isString2) {
				valueFilter = Util.padLeftZeros(valueFilter, 25);
				// if value is equality search
				if (low_key1 == null) {// In row range search if low key is null ([null,string2])
					low_key = new StringKey(Util.padRightZeros("", 25) + ":" + valueFilter);
					high_key = new StringKey(Util.padRightZeros(high_key1, 25) + ":" + valueFilter);
				} else if (high_key1 == null) {// In row range search if high key is null([string1,null])
					low_key = new StringKey(Util.padRightZeros(low_key1, 25) + ":" + valueFilter);
					high_key = null;
				} else {
					low_key = new StringKey(Util.padRightZeros(low_key1, 25) + ":" + valueFilter);
					high_key = new StringKey(Util.padRightZeros(high_key1, 25) + ":" + valueFilter);
				}

			} else {// if both are range searches
				if (low_key1 == null) { // like [null, highkey1] [lowkey2,highkey2]
					if (low_key2 == null)
						low_key = null;
					else
						low_key = new StringKey(Util.padRightZeros("", 25) + ":" + Util.padLeftZeros(low_key2, 25));
					if (high_key2 == null) // like [null, highkey1] [lowkey2,null]
						high_key = new StringKey(Util.padRightZeros(high_key1, 25) + ":~");
					else
						high_key = new StringKey(
								Util.padRightZeros(high_key1, 25) + ":" + Util.padLeftZeros(high_key2, 25));
				} else if (high_key1 == null) {// like [lowkey1, null] [lowkey2,highkey2]
					high_key = null;
					if (low_key2 == null)// like [lowkey1, null] [null,highkey2]
						low_key = new StringKey(Util.padRightZeros(low_key1, 25) + ":");
					else
						low_key = new StringKey(
								Util.padRightZeros(low_key1, 25) + ":" + Util.padLeftZeros(low_key2, 25));
				} else { // // like [lowkey1, highkey1] [lowkey2,highkey2]
					low_key = new StringKey(Util.padRightZeros(low_key1, 25) + ":" + Util.padLeftZeros(low_key2, 25));
					high_key = new StringKey(
							Util.padRightZeros(high_key1, 25) + ":" + Util.padLeftZeros(high_key2, 25));
				}
			}
		}

		BTFileScan scan = Map_MTree.new_scan(low_key, high_key);

		while ((entry1 = scan.get_next()) != null) {
			result = true;

			mapid = ((LeafData) entry1.data).getData();
			record = Map_HF.getRecord(mapid);
			rowlabel = record.getRowLabel();
			columnlabel = record.getColumnLabel();
			value = record.getValue();

			if (!ColumnLabel_null && result) {
				result = columnLabelCheck(columnFilter, columnlabel);
			}

			int valueInInt = Integer.parseInt(value);

			if (!Value_null && result) {
				result = valueLabelCheck(valueFilter, valueInInt);
			}

			if (result) {
				Result_HF.insertMap(record.getMapByteArray());
			}
		}
		scan.DestroyBTreeFileScan();
		Map_MTree.close();
		Map_MTree1.close();
	}

	private void streamByColumnRow(bigT bigHeapFile, String rowFilter, String columnFilter, String valueFilter)
			throws Exception {
		boolean result = true;
		KeyDataEntry entry1 = null;
		MID mapid = null;
		String rowlabel = null, columnlabel = null, value = null;
		Map record = null;

		bigT Map_HF = bigHeapFile;

		int keytype = AttrType.attrString;
		BTreeFile Map_MTree = new BTreeFile(bigHeapFile._fileName + "/Map_BTreeIndex", keytype, 56, 1);

		int integerType = AttrType.attrInteger;
		BTreeFile Map_MTree1 = new BTreeFile(bigHeapFile._fileName + "/Map_BTreeIndex1", integerType, 4, 1);

		KeyClass low_key = null;
		KeyClass high_key = null;

		String low_key1 = null;
		String high_key1 = null;

		String low_key2 = null;
		String high_key2 = null;

		Boolean isString1 = false;
		Boolean isString2 = false;

		if (columnFilter.startsWith("[")) {
			StringBuilder str = new StringBuilder(columnFilter);
			String[] strarray = ((str.deleteCharAt(str.length() - 1)).deleteCharAt(0)).toString().split(",");
			if (!strarray[0].equalsIgnoreCase("null"))
				low_key1 = strarray[0];
			if (!strarray[1].equalsIgnoreCase("null"))
				high_key1 = strarray[1];
		}

		else {
			isString1 = true;
		}

		if (rowFilter.startsWith("[")) {
			StringBuilder str = new StringBuilder(rowFilter);
			String[] strarray = ((str.deleteCharAt(str.length() - 1)).deleteCharAt(0)).toString().split(",");
			if (!strarray[0].equalsIgnoreCase("null"))
				low_key2 = strarray[0];
			if (!strarray[1].equalsIgnoreCase("null"))
				high_key2 = strarray[1];
		}

		else {
			isString2 = true;
		}

		if (isString1) {// if column is equality search
			if (isString2) {// if row is equality search
				// both are equality searches
				low_key = new StringKey(Util.padRightZeros(columnFilter, 25) + ":" + Util.padRightZeros(rowFilter, 25));
				high_key = new StringKey(
						Util.padRightZeros(columnFilter, 25) + ":" + Util.padRightZeros(rowFilter, 25));
			} else {
				// column is equality search and row is range range search
				if (low_key2 == null) {// In row range search if low key is null ([null,string2]) //[null,X]
										// Prakash//[null,Gnana] Prakash
					low_key = new StringKey(Util.padRightZeros(columnFilter, 25) + ":");// to fetch all the maps start
																						// with columnfilter: and
																						// greater than low key
					high_key = new StringKey(
							Util.padRightZeros(columnFilter, 25) + ":" + Util.padRightZeros(high_key2, 25));
				} else if (high_key2 == null) {// In row range search if high key is null([string1,null])//[D,null]
												// Prakash
					low_key = new StringKey(
							Util.padRightZeros(columnFilter, 25) + ":" + Util.padRightZeros(low_key2, 25));
					high_key = new StringKey(Util.padRightZeros(columnFilter, 25) + ":~");
				} else {
					// if row range search is like [string1,string2]//[Avvaru,Gnana] Prakash
					low_key = new StringKey(
							Util.padRightZeros(columnFilter, 25) + ":" + Util.padRightZeros(low_key2, 25));

					high_key = new StringKey(
							Util.padRightZeros(columnFilter, 25) + ":" + Util.padRightZeros(high_key2, 25));
				}
			}
		} else {
			// if column is range search
			if (isString2) {
				// if row is equality search
				if (low_key1 == null) {// In column range search if low key is null ([null,string2]) // Mahesh
										// [null,Zebra]
					// low_key=null;
					low_key = new StringKey(Util.padRightZeros("", 25) + ":" + Util.padRightZeros(rowFilter, 25));

					high_key = new StringKey(
							Util.padRightZeros(high_key1, 25) + ":" + Util.padRightZeros(rowFilter, 25));
				} else if (high_key1 == null) {// In column range search if high key is null([string1,null]) // Mahesh
												// [Prakash,null]
					low_key = new StringKey(Util.padRightZeros(low_key1, 25) + ":" + Util.padRightZeros(rowFilter, 25));
					high_key = null;
				} else { // Mahesh [Prakash,Yellow]

					low_key = new StringKey(Util.padRightZeros(low_key1, 25) + ":" + Util.padRightZeros(rowFilter, 25));
					high_key = new StringKey(
							Util.padRightZeros(high_key1, 25) + ":" + Util.padRightZeros(rowFilter, 25));
				}

			} else {// if both are range searches
				if (low_key1 == null) { // like [null, highkey1] [lowkey2,highkey2]
					if (low_key2 == null)
						low_key = null;
					else
						low_key = new StringKey(Util.padRightZeros("", 25) + ":" + Util.padRightZeros(low_key2, 25));
					if (high_key2 == null) // like [null, highkey1] [lowkey2,null]

						high_key = new StringKey(Util.padRightZeros(high_key1, 25) + ":~");
					else
						high_key = new StringKey(
								Util.padRightZeros(high_key1, 25) + ":" + Util.padRightZeros(high_key2, 25));
				} else if (high_key1 == null) {// like [lowkey1, null] [lowkey2,highkey2]
					high_key = null;
					if (low_key2 == null)// like [lowkey1, null] [null,highkey2]

						low_key = new StringKey(Util.padRightZeros(low_key1, 25) + ":");
					else
						low_key = new StringKey(
								Util.padRightZeros(low_key1, 25) + ":" + Util.padRightZeros(low_key2, 25));
				} else { // like [lowkey1, highkey1] [lowkey2,highkey2]

					low_key = new StringKey(Util.padRightZeros(low_key1, 25) + ":" + Util.padRightZeros(low_key2, 25));
					high_key = new StringKey(
							Util.padRightZeros(high_key1, 25) + ":" + Util.padRightZeros(high_key2, 25));
				}
			}
		}

		BTFileScan scan = Map_MTree.new_scan(low_key, high_key);

		while ((entry1 = scan.get_next()) != null) {
			result = true;

			mapid = ((LeafData) entry1.data).getData();
			record = (Map) Map_HF.getRecord(mapid);
			rowlabel = record.getRowLabel();
			columnlabel = record.getColumnLabel();
			value = record.getValue();

			if (!RowLabel_null && result) {
				result = rowLabelCheck(rowFilter, rowlabel);
			}

			int valueInInt = Integer.parseInt(value);

			if (!Value_null && result) {
				result = valueLabelCheck(valueFilter, valueInInt);
			}

			if (result) {

				Result_HF.insertMap(record.getMapByteArray());
			}
		}
		scan.DestroyBTreeFileScan();
		Map_MTree.close();
		Map_MTree1.close();
	}

	// To scan entire file
	private void ScanEntirebigT(bigT bigHeapFile, String rowFilter, String columnFilter, String valueFilter) {
		try {
			RowLabelFilter = rowFilter;
			ColumnLabelFilter = columnFilter;
			ValueFilter = valueFilter;
			boolean result = true;

			String rowlabel = null, columnlabel = null, value = null;

			bigT Map_HF = bigHeapFile;
			BScan am = new BScan(Map_HF); // Creating a scanning object on Map_HF
			Map map = null;

			MID mid = new MID();

			while ((map = (Map) am.getNext(mid)) != null) {
				result = true;
				rowlabel = map.getRowLabel();
				columnlabel = map.getColumnLabel();
				value = map.getValue();
				if (!RowLabel_null && result) {
					result = rowLabelCheck(rowFilter, rowlabel);
				}

				if (!ColumnLabel_null && result) {
					result = columnLabelCheck(columnFilter, columnlabel);
				}

				int valueInInt = Integer.parseInt(value);

				if (!Value_null && result) {
					result = valueLabelCheck(valueFilter, valueInInt);
				}

				if (result) {
					Result_HF.insertMap(map.getMapByteArray());
				}
			}
			am.closescan();
		} catch (Exception e) {
			System.err.println("Error scanning entire heap file for query::" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
	}

	public Boolean rowLabelCheck(String rowFilter, String rowlabel) {

		Boolean result = true;

		if (rowFilter.startsWith("[")) {

			String leftString = null;
			String rightString = null;
			StringBuilder str = new StringBuilder(rowFilter);
			String[] strarray = ((str.deleteCharAt(str.length() - 1)).deleteCharAt(0)).toString().split(",");

			if (!strarray[0].equalsIgnoreCase("null"))
				leftString = strarray[0];
			if (!strarray[1].equalsIgnoreCase("null"))
				rightString = strarray[1];
			if (leftString == null && rightString != null)
				result = result && (rowlabel.compareTo(rightString) <= 0);
			else if (leftString != null && rightString == null)
				result = result && (leftString.compareTo(rowlabel) <= 0);
			else {
				result = result && (leftString.compareTo(rowlabel) <= 0) && (rowlabel.compareTo(rightString) <= 0);
			}
		} else
			result = result && (rowFilter.compareTo(rowlabel) == 0);

		return result;
	}

	public Boolean columnLabelCheck(String columnFilter, String columnlabel) {

		Boolean result = true;

		if (columnFilter.startsWith("[")) {

			String leftString = null;
			String rightString = null;
			StringBuilder str = new StringBuilder(columnFilter);
			String[] strarray = ((str.deleteCharAt(str.length() - 1)).deleteCharAt(0)).toString().split(",");

			if (!strarray[0].equalsIgnoreCase("null"))
				leftString = strarray[0];
			if (!strarray[1].equalsIgnoreCase("null"))
				rightString = strarray[1];
			if (leftString == null && rightString != null)
				result = result && (columnlabel.compareTo(rightString) <= 0);
			else if (leftString != null && rightString == null)
				result = result && (leftString.compareTo(columnlabel) <= 0);
			else {
				result = result && (leftString.compareTo(columnlabel) <= 0)
						&& (columnlabel.compareTo(rightString) <= 0);
			}
		} else
			result = result && (columnFilter.compareTo(columnlabel) == 0);

		return result;

	}

	public Boolean valueLabelCheck(String valueFilter, int valueInInt) {

		Boolean result = true;

		if (valueFilter.startsWith("[")) {
			String leftString = null;
			String rightString = null;
			int leftValue = 0;
			int rightValue = 0;
			StringBuilder str = new StringBuilder(valueFilter);
			String[] strarray = ((str.deleteCharAt(str.length() - 1)).deleteCharAt(0)).toString().split(",");

			if (!strarray[0].equalsIgnoreCase("null")) {
				leftString = strarray[0];
				leftValue = Integer.parseInt(leftString);
			}
			if (!strarray[1].equalsIgnoreCase("null")) {
				rightString = strarray[1];
				rightValue = Integer.parseInt(rightString);
			}
			if (leftString == null && rightString != null)
				result = result && (valueInInt <= rightValue);
			else if (leftString != null && rightString == null)
				result = result && (leftValue <= valueInInt);
			else
				result = result && (leftValue <= valueInInt) && (valueInInt <= rightValue);

		} else
			result = result && (Integer.parseInt(valueFilter) == valueInInt);

		return result;

	}

	// Order Type
	public MapOrder get_Sort_order(int Sortoption) {
		MapOrder Sort_order = null;

		switch (Sortoption) {
		case 1:
			Sort_order = new MapOrder(MapOrder.RowColumnTimestamp);
			break;

		case 2:
			Sort_order = new MapOrder(MapOrder.ColumnRowTimestamp);
			break;

		case 3:
			Sort_order = new MapOrder(MapOrder.RowTimestamp);
			break;

		case 4:
			Sort_order = new MapOrder(MapOrder.ColumnTimestamp);
			break;

		case 5:
			Sort_order = new MapOrder(MapOrder.Timestamp);
			break;

		}
		return Sort_order;
	}

	// To close a stream
	public void closeStream() {
		try {
			if (Result_HF != null) {
				Result_HF.deleteFile();
			}

		} catch (Exception e) {
			System.out.println("Error closing Stream" + e);
		}
	}

	public Map getNext(MID mid)
			throws InvalidSlotNumberException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		Map rmap = null;
		if (Result_HF != null) {
			rmap = Result_HF.getMap(mid);
		}
		return rmap;
	}

}// End of Stream class
