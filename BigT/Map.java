package BigT;

/* File Map.java */

import java.io.*;
import java.lang.*;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidMapSizeException;
import heap.InvalidTypeException;

public class Map implements GlobalConst {

	/**
	 * Maximum size of any map
	 */
	public static final int max_size = MINIBASE_PAGESIZE;

	private static final int BigT_MAP_SIZE = 54;

	/**
	 * a byte array to hold data
	 */
	public byte[] data;

	/**
	 * start position of this Map in data[]
	 */
	private int map_offset;

	/**
	 * length of this Map
	 */
	private int map_length;

	/**
	 * private field Number of fields in this Map
	 */
	private short fldCnt = 4;

	/**
	 * private field Array of offsets of the fields
	 */

	private short[] fldOffset;

	/**
	 * Class constructor Create a new Map with length = max_size,Map offset = 0.
	 */

	private String RowLabel; // 22 bytes
	private String ColumnLabel; // 18 bytes
	private int TimeStamp; // 4 bytes
	private String Value; // 10 bytes

	// Constructor
	// Create a new Map with default map size
	public Map() {
		data = new byte[BigT_MAP_SIZE];
		map_offset = 0;
		map_length = BigT_MAP_SIZE;
	}

	// Constructor
	// Create a new map with respective size
	public Map(int size) {
		data = new byte[BigT_MAP_SIZE];
		map_offset = 0;
		map_length = size;
	}

	/**
	 * Constructor
	 * 
	 * @param aMap   a byte array which contains the Map
	 * @param offset the offset of the Map in the byte array
	 * @throws IOException
	 */
	public Map(byte[] amap, int offset) throws IOException {
		data = amap;
		map_offset = offset;
	}

	public Map(byte[] amap, int offset, int length) throws IOException {
		data = amap;
		map_offset = offset;
		map_length = length;
	}

	/**
	 * Constructor(used as Map copy)
	 * 
	 * @param fromMap a byte array which contains the Map
	 * @throws IOException
	 * 
	 */
	public Map(Map fromMap) throws IOException {
		data = fromMap.getMapByteArray();
		map_length = fromMap.getLength();
		map_offset = 0;
		setRowLabel(fromMap.getRowLabel());
		setColumnLabel(fromMap.getColumnLabel());
		setTimeStamp(fromMap.getTimeStamp());
		setValue(fromMap.getValue());
		Convert.setStrValue(fromMap.getRowLabel(), 0, data);
		Convert.setStrValue(fromMap.getColumnLabel(), 22, data);
		Convert.setIntValue(fromMap.getTimeStamp(), 40, data);
		Convert.setStrValue(fromMap.getValue(), 44, data);
	}

	/**
	 * Copy a Map to the current Map position you must make sure the Map lengths
	 * must be equal
	 * 
	 * @param fromMap the Map being copied
	 */
	public void mapCopy(Map fromMap) {
		try {
			// System.out.println(fromMap.getColumnLabel()+"in map copy");
			byte[] temparray = fromMap.getMapByteArray();
			// System.out.println(map_offset+"offset and length"+map_length);
			// System.arraycopy(temparray, 0, data, map_offset, map_length);
			System.arraycopy(temparray, 0, data, 0, 54);
			// System.out.println(map_length+"in map copy");
			RowLabel = fromMap.getRowLabel();
			ColumnLabel = fromMap.getColumnLabel();
			TimeStamp = fromMap.getTimeStamp();
			Value = fromMap.getValue();
		} catch (Exception e) {
			System.out.println("Error in map copy");
		}
	}
	public void mapCopy1(Map fromMap) {
		try {
			// System.out.println(fromMap.getColumnLabel()+"in map copy");
			byte[] temparray = fromMap.getMapByteArray();
			// System.out.println(map_offset+"offset and length"+map_length);
			// System.arraycopy(temparray, 0, data, map_offset, map_length);
			System.arraycopy(temparray, 0, data, map_offset, map_length);
			// System.out.println(map_length+"in map copy");
			RowLabel = fromMap.getRowLabel();
			ColumnLabel = fromMap.getColumnLabel();
			TimeStamp = fromMap.getTimeStamp();
			Value = fromMap.getValue();
		} catch (Exception e) {
			System.out.println("Error in map copy");
		}
	}

	/** This is used when you don’t want to use the constructor */
	public void mapInit(byte[] amap, int offset, int length) {
		data = amap;
		map_offset = offset;
		map_length = length;
	}

	/**
	 * Set a Map with the given Map length and offset
	 * 
	 * @param record a byte array contains the Map
	 * @param offset the offset of the Map ( =0 by default)
	 * @param length the length of the Map
	 */
	public void mapSet(byte[] record, int offset, int length) {
		// System.out.println(record.length+"record length"+data.length+":data length");
		System.arraycopy(record, offset, data, 0, BigT_MAP_SIZE);
		map_offset = 0;
		map_length = length;
	}

	public int getOffset() {
		return map_offset;
	}

	public String getRowLabel() {
		String val = null;
		try {
			val = Convert.getStrValue(0, data, 22);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return val;
	}

	public String getColumnLabel() {
		String val = null;
		try {
			val = Convert.getStrValue(22, data, 18);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return val;
	}

	public int getTimeStamp() {
		int val = 0;
		try {
			val = Convert.getIntValue(40, data);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return val;
	}

	public String getValue() {
		String val = null;
		try {
			val = Convert.getStrValue(44, data, 10);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return val;
	}

	public void setRowLabel(String rowlabel) throws IOException {
		RowLabel = rowlabel;
		Convert.setStrValue(rowlabel, 0, data);
	}

	public void setColumnLabel(String columnlabel) throws IOException {
		ColumnLabel = columnlabel;
		Convert.setStrValue(columnlabel, 22, data);
	}

	public void setTimeStamp(int timestamp) throws IOException {
		TimeStamp = timestamp;
		Convert.setIntValue(timestamp, 40, data);
	}

	public void setValue(String value) throws IOException {
		Value = value;
		Convert.setStrValue(value, 44, data);
	}

	/**
	 * get the length of a Map, call this method if you did not call setHdr ()
	 * before
	 * 
	 * @return length of this Map in bytes
	 */
	public int getLength() {
		return map_length;
	}

	/**
	 * get the length of a Map, call this method if you did call setHdr () before
	 * 
	 * @return size of this Map in bytes
	 */
	public short size() {
		return ((short) (map_length));
	}

	/**
	 * Copy the Map byte array out
	 * 
	 * @return byte[], a byte array contains the Map the length of byte[] = length
	 *         of the Map
	 */

	public byte[] getMapByteArray() {
		byte[] mapcopy = new byte[map_length];
		System.arraycopy(data, map_offset, mapcopy, 0, map_length);
		return mapcopy;
	}

	/**
	 * return the data byte array
	 * 
	 * @return data byte array
	 */

	public byte[] returnMapByteArray() {
		return data;
	}

	/**
	 * Convert this field into integer
	 * 
	 * @param fldNo the field number
	 * @return the converted integer if success
	 * 
	 * @exception IOException                    I/O errors
	 * @exception FieldNumberOutOfBoundException Map field number out of bound
	 */

	public int getIntFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
		int val = 0;
		if (fldNo == 3) {
			val = Convert.getIntValue(40, data);
			return val;
		} else
			throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
	}

	/**
	 * Convert this field into String
	 *
	 * @param fldNo the field number
	 * @return the converted string if success
	 * 
	 * @exception IOException                    I/O errors
	 * @exception FieldNumberOutOfBoundException Map field number out of bound
	 */

	public String getStrFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
		String val = null;
		// System.out.println(fldNo+" kkkkkkkkkkkkkkk:::::::;"+fldCnt);
		if ((fldNo > 0) && (fldNo <= fldCnt)) {
			byte[] temparray = data;
			if (fldNo == 1)
				val = Convert.getStrValue(0, data, 22); // strlen+2
			else if (fldNo == 2)
				val = Convert.getStrValue(22, data, 18);
			else if (fldNo == 4)
				val = Convert.getStrValue(44, data, 10);
			return val;

		} else
			throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
	}

	/**
	 * Set this field to integer value
	 *
	 * @param fldNo the field number
	 * @param val   the integer value
	 * @exception IOException                    I/O errors
	 * @exception FieldNumberOutOfBoundException Map field number out of bound
	 */

	public Map setIntFld(int fldNo, int val) throws IOException, FieldNumberOutOfBoundException {
		if ((fldNo == 3)) {
			Convert.setIntValue(val, 40, data);
			return this;
		} else
			throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
	}

	/**
	 * Constructor(used as Map copy)
	 * 
	 * @param fromMap a byte array which contains the Map
	 * 
	 */

	/**
	 * Set this field to String value
	 *
	 * @param fldNo the field number
	 * @param val   the string value
	 * @exception IOException                    I/O errors
	 * @exception FieldNumberOutOfBoundException Map field number out of bound
	 */

	public Map setStrFld(int fldNo, String val) throws IOException, FieldNumberOutOfBoundException {
		if ((fldNo == 1)) {
			Convert.setStrValue(val, 0, data);
			return this;
		} else if (fldNo == 2) {
			Convert.setStrValue(val, 22, data);
			return this;
		} else if (fldNo == 4) {
			Convert.setStrValue(val, 44, data);
			return this;
		} else
			throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
	}

	/**
	 * Convert this field in to float
	 *
	 * @param fldNo the field number
	 * @return the converted float number if success
	 * 
	 * @exception IOException                    I/O errors
	 * @exception FieldNumberOutOfBoundException Map field number out of bound
	 */

	public float getFloFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
		float val;
		if ((fldNo > 0) && (fldNo <= fldCnt)) {
			val = Convert.getFloValue(fldOffset[fldNo - 1], data);
			return val;
		} else
			throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
	}

	/**
	 * Set this field to float value
	 *
	 * @param fldNo the field number
	 * @param val   the float value
	 * @exception IOException                    I/O errors
	 * @exception FieldNumberOutOfBoundException Map field number out of bound
	 */

	public Map setFloFld(int fldNo, float val) throws IOException, FieldNumberOutOfBoundException {
		if ((fldNo > 0) && (fldNo <= fldCnt)) {
			Convert.setFloValue(val, fldOffset[fldNo - 1], data);
			return this;
		} else
			throw new FieldNumberOutOfBoundException(null, "Map:Map_FLDNO_OUT_OF_BOUND");
	}

	/**
	 * setHdr will set the header of this Map.
	 *
	 * @param numFlds    number of fields
	 * @param types[]    contains the types that will be in this Map
	 * @param strSizes[] contains the sizes of the string
	 * 
	 * @exception IOException             I/O errors
	 * @exception InvalidTypeException    Invalid tupe type
	 * @exception InvalidMapSizeException Map size too big
	 *
	 */

	public void setHdr(short numFlds, AttrType types[], short strSizes[])
			throws IOException, InvalidTypeException, InvalidMapSizeException {

		if ((numFlds + 2) * 2 > max_size)
			throw new InvalidMapSizeException(null, "Map: Map_TOOBIG_ERROR");

		fldCnt = numFlds;
		Convert.setShortValue(numFlds, map_offset, data);
		fldOffset = new short[numFlds + 1];
		int pos = map_offset + 2; // start position for fldOffset[]

		fldOffset[0] = (short) ((numFlds + 2) * 2 + map_offset);

		Convert.setShortValue(fldOffset[0], pos, data);
		pos += 2;
		short strCount = 0;
		short incr;
		int i;

		for (i = 1; i < numFlds; i++) {
			switch (types[i - 1].attrType) {

			case AttrType.attrInteger:
				incr = 4;
				break;

			case AttrType.attrString:
				incr = (short) (strSizes[strCount] + 2);
				strCount++;
				break;

			default:
				throw new InvalidTypeException(null, "Map: Map_TYPE_ERROR");
			}
			fldOffset[i] = (short) (fldOffset[i - 1] + incr);
			Convert.setShortValue(fldOffset[i], pos, data);
			pos += 2;

		}
		switch (types[numFlds - 1].attrType) {

		case AttrType.attrInteger:
			incr = 4;
			break;

		case AttrType.attrString:
			incr = (short) (strSizes[strCount] + 2); // strlen in bytes = strlen +2
			break;

		default:
			throw new InvalidTypeException(null, "Map: Map_TYPE_ERROR");
		}

		fldOffset[numFlds] = (short) (fldOffset[i - 1] + incr);
		Convert.setShortValue(fldOffset[numFlds], pos, data);

		map_length = fldOffset[numFlds] - map_offset;
		// System.out.println("In header map length"+map_length);
		if (map_length > max_size)
			throw new InvalidMapSizeException(null, "Map: Map_TOOBIG_ERROR");
		// System.out.println("in header,,,,,,,,,,,,,,,,,,,,,,,");
	}

	/**
	 * Returns number of fields in this Map
	 *
	 * @return the number of fields in this Map
	 *
	 */

	public short noOfFlds() {
		return fldCnt;
	}

	/**
	 * Makes a copy of the fldOffset array
	 *
	 * @return a copy of the fldOffset arrray
	 *
	 */

	public short[] copyFldOffset() {
		short[] newFldOffset = new short[fldCnt + 1];
		for (int i = 0; i <= fldCnt; i++) {
			newFldOffset[i] = fldOffset[i];
		}
		return newFldOffset;
	}

	/**
	 * Print out the Map
	 * 
	 * @param type the types in the Map
	 * @Exception IOException I/O exception
	 */
	public void print() {
		try {
			System.out.println(this.getRowLabel() + "," + this.getColumnLabel() + "," + this.getTimeStamp() + "->"
					+ this.getValue());
		} catch (Exception e) {
			System.out.println("Error in print map" + e);
		}
	}
}