
package BigTIterator;

/**
 * An element in the binary tree.
 * including pointers to the children, the parent in addition to the item.
 */
public class BpnodeSplayNode
{
  /** a reference to the element in the node */
  public Bpnode             item;

  /** the left child pointer */
  public BpnodeSplayNode    lt;

  /** the right child pointer */
  public BpnodeSplayNode    rt;

  /** the parent pointer */
  public BpnodeSplayNode    par;

  /**
   * class constructor, sets all pointers to <code>null</code>.
   * @param h the element in this node
   */
  public BpnodeSplayNode(Bpnode h) 
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
  public BpnodeSplayNode(Bpnode h, BpnodeSplayNode l, BpnodeSplayNode r) 
  {
    item = h;
    lt = l;
    rt = r;
    par = null;
  }

  /** a static dummy node for use in some methods */
  public static BpnodeSplayNode dummy = new BpnodeSplayNode(null);
  
}

