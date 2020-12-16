package BDescIterator;


import chainexception.*;

import java.lang.*;

public class BDUtilsException extends ChainException {
  public BDUtilsException(String s){super(null,s);}
  public BDUtilsException(Exception prev, String s){ super(prev,s);}
}

