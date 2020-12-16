package BigTIterator;




import chainexception.*;
import java.lang.*;

public class BIteratorBMException extends ChainException 
{
  public BIteratorBMException(String s){super(null,s);}
  public BIteratorBMException(Exception prev, String s){ super(prev,s);}
}
