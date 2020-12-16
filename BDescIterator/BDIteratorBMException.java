package BDescIterator;






import chainexception.*;
import java.lang.*;

public class BDIteratorBMException extends ChainException 
{
  public BDIteratorBMException(String s){super(null,s);}
  public BDIteratorBMException(Exception prev, String s){ super(prev,s);}
}
