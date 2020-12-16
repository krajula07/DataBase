package BigTIterator;
import chainexception.*;

import java.lang.*;

public class BUtilsException extends ChainException {
  public BUtilsException(String s){super(null,s);}
  public BUtilsException(Exception prev, String s){ super(prev,s);}
}
