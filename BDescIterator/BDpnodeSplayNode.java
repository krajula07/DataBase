package BDescIterator;



/**
 * An element in the binary tree.
 * including pointers to the children, the parent in addition to the item.
 */
public class BDpnodeSplayNode
{
  /** a reference to the element in the node */
  public BDpnode             item;

  /** the left child pointer */
  public BDpnodeSplayNode    lt;

  /** the right child pointer */
  public BDpnodeSplayNode    rt;

  /** the parent pointer */
  public BDpnodeSplayNode    par;

  /**
   * class constructor, sets all pointers to <code>null</code>.
   * @param h the element in this node
   */
  public BDpnodeSplayNode(BDpnode h) 
  {
    item = h;
    lt = null;
    rt = null;
    par = null;
  }

  /**
   * class constructor, sets all pointers.
   * @param h the element in this node
   * @param l left child pointer
   * @param r right child pointer
   */  
  public BDpnodeSplayNode(BDpnode h, BDpnodeSplayNode l, BDpnodeSplayNode r) 
  {
    item = h;
    lt = l;
    rt = r;
    par = null;
  }

  /** a static dummy node for use in some methods */
  public static BDpnodeSplayNode dummy = new BDpnodeSplayNode(null);
  
}

