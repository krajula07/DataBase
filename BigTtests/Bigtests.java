package BigTtests;

import java.util.Scanner;
import diskmgr.bigDB;

public class Bigtests {
  
	public static Boolean batchinsert;

    public static void main(String[] args) throws Exception {

        while (true) {
            Scanner in = new Scanner(System.in);
            System.out.println("**************************************************************************");
            System.out.println("Please input your command: ");
            String command = in.nextLine();
            System.out.println(command);
            String[] arguments = command.split(" ");
            if (arguments.length == 1 && arguments[0].equals("help")) {
                System.out.println("This bigDB supports commands in below format:-");
                System.out.println("- batchinsert DATAFILENAME TYPE BIGTABLENAME NUMBUF");
                System.out.println("- query BIGTABLENAME TYPE ORDERTYPE ROWFILTER COLUMNFILTER VALUEFILTER NUMBUF");

                System.out.println();
                System.out.println(" bigDB closed.");
                break;
            } else if (arguments.length == 5 && arguments[0].equals("batchinsert")) {
            	batchinsert=true;
                BatchInsert.runBatchInsert(arguments); // To execute the batch insert command
             
            }else if (arguments.length == 7 && arguments[0].equals("query")) {
            	batchinsert=false;
                SimpleQuery.runSimpleNodeQuery(arguments); // To execute query
            } else if (arguments.length == 6 && arguments[0].equals("rowjoin")) {
            	System.out.println("Executing the input query...");
                SimpleQuery.runJoinQuery(arguments); // To execute query
            }else if (arguments.length==2 && arguments[0].equals("getCounts"))
            {       
                bigDB.mergeBigT();
            	
            }else if (arguments.length == 6 && arguments[0].equals("rowsort")) {
            	batchinsert=false;
                RowSort.runRowSort(arguments); // To execute query
            } else if (arguments.length == 8 && arguments[0].equals("mapinsert")) {
            	batchinsert=false;
            	bigDB.runMapInsert(arguments); // To execute query
            } else {
                System.out.println("Invalid command! Please input again!"); 
                System.out.println();
            }
        }
    }
}
