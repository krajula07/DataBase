package BigTtests;

import global.GlobalConst;
import global.SystemDefs;

import java.io.File;

public class BigTDBManager implements GlobalConst {

	private String dbPath;
	private SystemDefs sysdef;

	public BigTDBManager() {
	}

	public void init(String dbname, int indexType) {
		dbPath = dbname;
		File file = new File(dbPath);
		// check if the database is already exists
		if (file.exists()) { // Open existing DB
			sysdef = new SystemDefs(dbPath, 1000, "Clock");
		} else { // Create a new One
			sysdef = new SystemDefs(dbPath, 100000, 1000, "Clock", indexType);
		}
	}

	public void init(String dbname, int indexType, int NUMBUF) {
		dbPath = dbname;
		File file = new File(dbPath);
		if (file.exists()) { // Open existing DB
			sysdef = new SystemDefs(dbPath, NUMBUF, "Clock");
		} else { // Create a new One
			sysdef = new SystemDefs(dbPath, 100000, NUMBUF, "Clock", indexType);
		}
	}

	public void close() {
		sysdef.close();
	}
}
