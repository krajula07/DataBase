package BigT;

import java.util.HashSet;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import BigTtests.BigTDBManager;
import BigTtests.Util;
import btree.BTFileScan;
import btree.BTreeFile;
import btree.KeyClass;
import btree.KeyDataEntry;
import btree.LeafData;
import btree.StringKey;
import global.AttrType;
import global.MID;
import global.SystemDefs;

public class RowJoin {

	public RowJoin(int numbuf, String bigTable1, String bigTable2, String outBigt, String columnFilter) {
		bigT bhf_1 = null, bhf_2 = null, bhf_3 = null, bhf_4 = null, bhf_5 = null, bhf_6 = null, bhf_7 = null,
				bhf_8 = null, bhf_9 = null, bhf_10 = null;
		BTreeFile bigT1Tree = null, bigT2Tree = null;
		BScan bscan = null;
		KeyClass key;

//        System.out.println("Unpinned1: "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers() ); 
//        System.out.println("Total1: "+SystemDefs.JavabaseBM.getNumBuffers());

		try {
			bhf_1 = new bigT(bigTable1 + "_1");
			bhf_2 = new bigT(bigTable1 + "_2");
			bhf_3 = new bigT(bigTable1 + "_3");
			bhf_4 = new bigT(bigTable1 + "_4");
			bhf_5 = new bigT(bigTable1 + "_5");
			bhf_6 = new bigT(bigTable2 + "_1");
			bhf_7 = new bigT(bigTable2 + "_2");
			bhf_8 = new bigT(bigTable2 + "_3");
			bhf_9 = new bigT(bigTable2 + "_4");
			bhf_10 = new bigT(bigTable2 + "_5");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Creating btree for bigtable1//

//        System.out.println("Unpinned2: "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers() ); 
//        System.out.println("Total2: "+SystemDefs.JavabaseBM.getNumBuffers());
		if(bigTable1.compareTo(bigTable2)==0) {
			bigTable2="self"; // SELF JOIN
		}
		
		try {

			bigT1Tree = new BTreeFile(bigTable1 + "/BTIndex", AttrType.attrString, 85, 0);
			bigT1Tree.destroyFile();
			bigT1Tree = new BTreeFile(bigTable1 + "/BTIndex", AttrType.attrString, 85, 0);

			MID mid = new MID();
			Map map = new Map();

			if (bhf_1.getMapCnt() > 0) {
				System.out.println(bhf_1._fileName + " has " + bhf_1.getMapCnt() + " maps");
				bscan = bhf_1.openScan();
				while ((map = bscan.getNext(mid)) != null) {
					key = new StringKey(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
							+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue());
					bigT1Tree.insert(key, mid);
				}
			}
			if (bhf_2.getMapCnt() > 0) {
				System.out.println(bhf_2._fileName + " has " + bhf_2.getMapCnt() + " maps");
				bscan = bhf_2.openScan();
				while ((map = bscan.getNext(mid)) != null) {
					key = new StringKey(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
							+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue());
					bigT1Tree.insert(key, mid);
				}
			}
			if (bhf_3.getMapCnt() > 0) {
				System.out.println(bhf_3._fileName + " has " + bhf_3.getMapCnt() + " maps");
				bscan = bhf_3.openScan();
				while ((map = bscan.getNext(mid)) != null) {
					key = new StringKey(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
							+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue());
					bigT1Tree.insert(key, mid);
				}
			}
			if (bhf_4.getMapCnt() > 0) {
				System.out.println(bhf_4._fileName + " has " + bhf_4.getMapCnt() + " maps");
				bscan = bhf_4.openScan();
				while ((map = bscan.getNext(mid)) != null) {
					key = new StringKey(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
							+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue());
					bigT1Tree.insert(key, mid);
				}

			}
			if (bhf_5.getMapCnt() > 0) {
				System.out.println(bhf_5._fileName + " has " + bhf_5.getMapCnt() + " maps");
				bscan = bhf_5.openScan();
				while ((map = bscan.getNext(mid)) != null) {
					key = new StringKey(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
							+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue());
					bigT1Tree.insert(key, mid);
				}

			}
			bscan.closescan();
			bigT1Tree.close();
//	        System.out.println("Unpinned3: "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers() ); 
//	        System.out.println("Total3: "+SystemDefs.JavabaseBM.getNumBuffers());

		}

		catch (Exception e) {
			e.printStackTrace();
		}
		// end of try catch for bigtable1 btree//

		// Creating btree for bigtable2//
		try {

			bigT2Tree = new BTreeFile(bigTable2 + "/BTIndex", AttrType.attrString, 85, 0);
			bigT2Tree.destroyFile();
			bigT2Tree = new BTreeFile(bigTable2 + "/BTIndex", AttrType.attrString, 85, 0);

			MID mid = new MID();
			Map map = new Map();
			if (bhf_6.getMapCnt() > 0) {
				System.out.println(bhf_6._fileName + " has " + bhf_6.getMapCnt() + " maps");
				bscan = bhf_6.openScan();
				while ((map = bscan.getNext(mid)) != null) {
					key = new StringKey(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
							+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue());
					bigT2Tree.insert(key, mid);
				}

			}
			if (bhf_7.getMapCnt() > 0) {
				System.out.println(bhf_7._fileName + " has " + bhf_7.getMapCnt() + " maps");
				bscan = bhf_7.openScan();
				while ((map = bscan.getNext(mid)) != null) {
					key = new StringKey(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
							+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue());
					bigT2Tree.insert(key, mid);
				}
			}
			if (bhf_8.getMapCnt() > 0) {
				System.out.println(bhf_8._fileName + " has " + bhf_8.getMapCnt() + " maps");
				bscan = bhf_8.openScan();
				while ((map = bscan.getNext(mid)) != null) {
					key = new StringKey(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
							+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue());
					bigT2Tree.insert(key, mid);
				}
			}
			if (bhf_9.getMapCnt() > 0) {
				System.out.println(bhf_9._fileName + " has " + bhf_9.getMapCnt() + " maps");
				bscan = bhf_9.openScan();
				while ((map = bscan.getNext(mid)) != null) {
					key = new StringKey(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
							+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue());
					bigT2Tree.insert(key, mid);
				}
			}
			if (bhf_10.getMapCnt() > 0) {
				System.out.println(bhf_10._fileName + " has " + bhf_10.getMapCnt() + " maps");
				bscan = bhf_10.openScan();
				while ((map = bscan.getNext(mid)) != null) {
					key = new StringKey(map.getRowLabel() + ":" + map.getColumnLabel() + ":"
							+ Util.padLeftZeros(map.getTimeStamp(), 25) + ":" + map.getValue());
					bigT2Tree.insert(key, mid);
				}
			}
			bscan.closescan();
			bigT2Tree.close();
		}

		catch (Exception e) {
			e.printStackTrace();
		}
//End of btree for bigtable2//	

		joinBigtTables(bigTable1, bigTable2, outBigt, columnFilter);
	}

	public void joinBigtTables(String bigTable1, String bigTable2, String outBigt, String columnFilter) {

		KeyDataEntry entry1 = null, prevEntry = null, currEntry = null;

		BTreeFile t1Tree, t2Tree;

		KeyClass key;

		BTFileScan scan = null;

		String curArray[], prevArray[];

		String currString, prevString;

//    System.out.println("Unpinned5: "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers() ); 
//    System.out.println("Total5: "+SystemDefs.JavabaseBM.getNumBuffers());

		try {
			BTreeFile bigT1Tree = new BTreeFile(bigTable1 + "/BTIndex", AttrType.attrString, 85, 0);
			BTreeFile bigT2Tree = new BTreeFile(bigTable2 + "/BTIndex", AttrType.attrString, 85, 0);

			t1Tree = new BTreeFile(bigTable1 + "/CVR1", AttrType.attrString, 66, 0);
			t1Tree.destroyFile();
			t1Tree = new BTreeFile(bigTable1 + "/CVR1", AttrType.attrString, 66, 0);
			t2Tree = new BTreeFile(bigTable2 + "/CVR2", AttrType.attrString, 66, 0);
			t2Tree.destroyFile();
			t2Tree = new BTreeFile(bigTable2 + "/CVR2", AttrType.attrString, 66, 0);

//    System.out.println("Unpinned6: "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers() ); 
//    
//    System.out.println("Total6: "+SystemDefs.JavabaseBM.getNumBuffers());

			// Creating another btree 't1Tree' with key format as column:value:row using
			// bigT1Tree

			scan = bigT1Tree.new_scan(null, null);

			prevEntry = scan.get_next();

			while ((currEntry = scan.get_next()) != null) {
				curArray = currEntry.key.toString().split(":");

				prevArray = prevEntry.key.toString().split(":");

				currString = curArray[0] + ":" + curArray[1];
				prevString = prevArray[0] + ":" + prevArray[1];

				if (currString.compareTo(prevString) != 0) {

					key = new StringKey(prevArray[1] + ":" + Util.padLeftZeros(prevArray[3], 20) + ":" + prevArray[0]);
					t1Tree.insert(key, ((LeafData) prevEntry.data).getData());
				}

				prevEntry = currEntry;
			}
			// For inserting last entry from bigT1Tree
			prevArray = prevEntry.key.toString().split(":");
			key = new StringKey(prevArray[1] + ":" + Util.padLeftZeros(prevArray[3], 20) + ":" + prevArray[0]);
			t1Tree.insert(key, ((LeafData) prevEntry.data).getData());

			scan.DestroyBTreeFileScan();
			bigT1Tree.close();
			t1Tree.close();

			// end of creating t1Tree//

			// Creating another btree 't2Tree' with key format as column:value:row using
			// bigT2Tree

			scan = bigT2Tree.new_scan(null, null);

			prevEntry = scan.get_next();

			while ((currEntry = scan.get_next()) != null) {
				curArray = currEntry.key.toString().split(":");

				prevArray = prevEntry.key.toString().split(":");

				currString = curArray[0] + ":" + curArray[1];
				prevString = prevArray[0] + ":" + prevArray[1];

				if (currString.compareTo(prevString) != 0) {

					key = new StringKey(prevArray[1] + ":" + Util.padLeftZeros(prevArray[3], 20) + ":" + prevArray[0]);
					t2Tree.insert(key, ((LeafData) prevEntry.data).getData());
				}

				prevEntry = currEntry;
			}
			// For inserting last entry from bigT2Tree
			prevArray = prevEntry.key.toString().split(":");
			key = new StringKey(prevArray[1] + ":" + Util.padLeftZeros(prevArray[3], 20) + ":" + prevArray[0]);
			t2Tree.insert(key, ((LeafData) prevEntry.data).getData());

			scan.DestroyBTreeFileScan();
			bigT2Tree.close();
			t2Tree.close();
			// end of creating t2Tree//

		} catch (Exception e) {
			e.printStackTrace();
		}
		startJoiningTables(bigTable1, bigTable2, outBigt, columnFilter);

	}

	public void startJoiningTables(String bigTable1, String bigTable2, String outBigt, String columnFilter) {

		BTFileScan scan1 = null, scan2 = null, scan3 = null, scan4 = null;

		HashSet<String> columnHashSet = new HashSet<String>();

		KeyDataEntry entry1 = null, entry2 = null, entry3 = null, entry4 = null;
		bigT outBigT;

//    System.out.println("Unpinned7 "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers() ); 
//    System.out.println("Total7: "+SystemDefs.JavabaseBM.getNumBuffers());

		try {

			outBigT = new bigT(outBigt + "_1");
			outBigT.deleteBigt();
			outBigT = new bigT(outBigt + "_1");

			BTreeFile bigT1Tree = new BTreeFile(bigTable1 + "/BTIndex", AttrType.attrString, 85, 0);
			BTreeFile bigT2Tree = new BTreeFile(bigTable2 + "/BTIndex", AttrType.attrString, 85, 0);

			BTreeFile t1Tree = new BTreeFile(bigTable1 + "/CVR1", AttrType.attrString, 66, 0);
			BTreeFile t2Tree = new BTreeFile(bigTable2 + "/CVR2", AttrType.attrString, 66, 0);

			KeyClass lowKey1 = new StringKey(columnFilter + ":");
			KeyClass highKey1 = new StringKey(columnFilter + ":~");

			KeyClass lowKey2 = new StringKey(columnFilter + ":");
			KeyClass highKey2 = new StringKey(columnFilter + ":~");

			// Scanning all the values staring with columnFilter in t1Tree
			scan1 = t1Tree.new_scan(lowKey1, highKey1);
			scan2 = t2Tree.new_scan(lowKey2, highKey2);

			while ((entry1 = scan1.get_next()) != null) {
				SortedMap<Integer, MID> duplicateMaps = new TreeMap<Integer, MID>();
				int value1 = Integer.parseInt((entry1.key.toString().split(":"))[1]);
				while ((entry2 = scan2.get_next()) != null) {
					int value2 = Integer.parseInt((entry2.key.toString().split(":"))[1]);
					if (value1 < value2) {
						scan2.DestroyBTreeFileScan();
						scan2 = t2Tree.new_scan(entry2.key, highKey2);
						break;
					} else if (value1 > value2)
						continue;
					else // values are equal
					{
						String row1 = (entry1.key.toString().split(":"))[2];
						String row2 = (entry2.key.toString().split(":"))[2];

						scan3 = bigT1Tree.new_scan(new StringKey(row1 + ":"), new StringKey(row1 + ":~"));
						scan4 = bigT2Tree.new_scan(new StringKey(row2 + ":"), new StringKey(row2 + ":~"));

						// scanning left table data to insert rows into out bigtable
						while ((entry3 = scan3.get_next()) != null) {

							String leftRowArray[] = entry3.key.toString().split(":");
							int timestamp1 = Integer.parseInt(leftRowArray[2]);
							String columnString1 = leftRowArray[1];
							Map map;
							try {
								map = new Map();
								map.setRowLabel(row1 + ":" + row2);
								map.setColumnLabel(columnString1);
								map.setTimeStamp(timestamp1);
								map.setValue(leftRowArray[3]);

								MID temp = outBigT.insertMap(map.getMapByteArray());

								columnHashSet.add(columnString1);

								if (columnFilter.compareTo(columnString1) == 0)
									duplicateMaps.put(timestamp1, temp);
							}

							catch (Exception e) {
								e.printStackTrace();
							}
						} // end of while loop for bigT1Tree scanning
						scan3.DestroyBTreeFileScan();
						// scanning right table data to insert rows into out bigtable

						while ((entry4 = scan4.get_next()) != null) {

							String RightRowArray[] = entry4.key.toString().split(":");
							int timestamp2 = Integer.parseInt(RightRowArray[2]);
							String columnString2 = RightRowArray[1];
							String column = columnString2;
							Map map;
							try {
								map = new Map();
								map.setRowLabel(row1 + ":" + row2);
								if (columnHashSet.contains(columnString2))
									columnString2 += "_Right";
								map.setColumnLabel(columnString2);
								map.setTimeStamp(timestamp2);
								map.setValue(RightRowArray[3]);

								// MID temp1=outBigT.insertMap(map.getMapByteArray());

								if (columnFilter.compareTo(column) == 0) {
									map.setColumnLabel(column);
									MID temp1 = outBigT.insertMap(map.getMapByteArray());
									duplicateMaps.put(timestamp2, temp1);
								} else
									outBigT.insertMap(map.getMapByteArray());
							} catch (Exception e) {
								e.printStackTrace();
							}
						} // end of while loop for bigT2Tree scanning
						scan4.DestroyBTreeFileScan();
					} // end of else
				} // end of while loop for t2Tree scanning

				String arr[] = (entry1.key.toString().split(":"));
				lowKey2 = new StringKey(arr[0] + ":" + arr[1] + ":");
				scan2.DestroyBTreeFileScan();
				scan2 = t2Tree.new_scan(lowKey2, highKey2);

				Iterator i5 = duplicateMaps.entrySet().iterator();

				while (duplicateMaps.size() > 3) {
					java.util.Map.Entry<Integer, MID> keyValuePair5 = (java.util.Map.Entry<Integer, MID>) i5.next();
					int key = keyValuePair5.getKey();
					outBigT.deleteRecord(duplicateMaps.get(key));
					i5.remove();
				}
			} // end of while loop for t1Tree scanning

//        System.out.println("Unpinned8: "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers() ); 
//        System.out.println("Total8: "+SystemDefs.JavabaseBM.getNumBuffers());
			scan1.DestroyBTreeFileScan();
			if (scan2 != null)
				scan2.DestroyBTreeFileScan();

			t1Tree.destroyFile();
			t2Tree.destroyFile();
			bigT1Tree.destroyFile();
			bigT2Tree.destroyFile();
			t1Tree.close();
			t2Tree.close();
			bigT1Tree.close();
			bigT2Tree.close();

			BScan am = new BScan(outBigT);
			Map map = null;
			MID mid = new MID();

			// System.out.println("Unpinned9:"+SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
			// );
			// System.out.println("Total9: "+SystemDefs.JavabaseBM.getNumBuffers());

			System.out.println("After Row Join Operation");

			// Inserting maps into the BTree with Row Label as key
			while ((map = (Map) am.getNext(mid)) != null) {
				System.out.println(map.getRowLabel() + ", " + map.getColumnLabel() + ", " + map.getTimeStamp() + ", "
						+ map.getValue());
			}
			System.out.println("Map count after join operation: " + outBigT.getMapCnt());
			am.closescan();

			// System.out.println("Unpinned10:
			// "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers() );
			// System.out.println("Total10: "+SystemDefs.JavabaseBM.getNumBuffers());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
