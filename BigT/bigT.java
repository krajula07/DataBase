package BigT;

import global.*;
import heap.*;
import java.io.IOException;

public class bigT extends Heapfile {

	String _bigtName;
	int _indexType;
	Stream stream;

	public bigT(String file, int type) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		super(file, type);
		_bigtName = file;
		_indexType = type;
	}

	public bigT(String file) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		super(file);
		_bigtName = file;
	}

	// Delete File
	public void deleteBigt() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidMapSizeException,
			HFBufMgrException, HFDiskMgrException, IOException {
		super.deleteFile();
	}

	// Delete Map
	public boolean deleteMap(MID mid) throws InvalidSlotNumberException, InvalidMapSizeException, HFException,
			HFBufMgrException, HFDiskMgrException, Exception {
		boolean status = super.deleteRecord(mid);
		return status;
	}

	// Count the number of Maps
	public int getMapCnt() throws InvalidSlotNumberException, InvalidMapSizeException, HFDiskMgrException,
			HFBufMgrException, IOException {
		int MapCounter = super.getRecCnt();
		return MapCounter;
	}

	// Get Map
	public Map getMap(MID mid) throws InvalidSlotNumberException, InvalidMapSizeException, HFException,
			HFDiskMgrException, HFBufMgrException, Exception {
		Map map = super.getRecord(mid);
		Map map1 = new Map(map);
		return map1;
	}

	// Insert Map
	public MID insertMap(byte[] mapPtr) throws Exception {
		MID mid = super.insertRecord(mapPtr);
		MID mid1 = new MID(mid.pageNo, mid.slotNo);
		return mid1;
	}

	// Update Map
	public boolean updateMap(MID mid, Map newMap) throws InvalidSlotNumberException, InvalidUpdateException,
			InvalidMapSizeException, HFException, HFDiskMgrException, HFBufMgrException, Exception {
		boolean status = super.updateRecord(mid, newMap);
		return status;
	}

	public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter) {
		try {
			// stream= new Stream(this,orderType,rowFilter,columnFilter,valueFilter);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stream;
	}

	// initilize a NScan
	public BScan openScan() throws InvalidMapSizeException, IOException {
		BScan newmapscan = new BScan(this);
		return newmapscan;
	}
}
