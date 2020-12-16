package BigTIterator;



import java.lang.*;
import chainexception.*;

public class BSortException extends ChainException 
{
  public BSortException(String s) {super(null,s);}
  public BSortException(Exception e, String s) {super(e,s);}
}
