package global;

/** 
 * MapOrder Enumeration class 
 * 
 */

public class MapOrder {

	public static final int Random = 0;
	public static final int Ascending = 1;
	public static final int Descending = 2;
  	public static final int RowColumnTimestamp = 3;
	public static final int ColumnRowTimestamp = 4;
	public static final int RowTimestamp = 5;
	public static final int ColumnTimestamp = 6;
	public static final int Timestamp = 7;
	public static final int Value = 8;
	public static final int HmapTrail=9;
  public int mapOrder;

  public MapOrder (int _mapOrder) {
    mapOrder = _mapOrder;
  }

  public String toString() {
    
    switch (mapOrder) {
    case Ascending:
    	return "Ascending";
    case Descending:
    	return "Descending";
    case RowColumnTimestamp:
	      return "RowColumnTimestamp";
	    case ColumnRowTimestamp:
	      return "ColumnRowTimestamp";
	    case RowTimestamp:
	      return "RowTimestamp";
	    case ColumnTimestamp:
		      return "ColumnTimestamp";
		    case Timestamp:
		      return "Timestamp";
		    case Value:
			      return "value";
		    case HmapTrail:
			      return "HmapTrail";
    }
    return ("Unexpected MapOrder " + mapOrder);
  }
  
	
	
  

  	 

  	  
  	  

  }

	

	

	

