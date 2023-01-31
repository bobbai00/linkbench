package com.facebook.LinkBench;

import jnr.ffi.annotations.In;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class LinkStoreJanusGraph extends GraphStore {

  public static final String CONFIG_FILENAME = "graphConfigFilename";

  public static final int DEFAULT_BULKINSERT_SIZE = 1024;

  String nodeLabel = "node";
  String linkLabel = "link";

  String graphConfigFilename;

  GraphTraversalSource g;


  Level debuglevel;

  private Phase phase;

  int bulkInsertSize = DEFAULT_BULKINSERT_SIZE;

  ReentrantLock lock = new ReentrantLock();
  long nodeIdIncrementer = 1;


  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  public LinkStoreJanusGraph() {
    super();
    // initialize with local graph
    JanusGraph localGraph = JanusGraphFactory.build().
            set("storage.backend", "berkeleyje").
            set("storage.directory", "/home/bob/Desktop/shahram-lab/ebay/data/graph").
            open();

    g = localGraph.traversal();
  }

  public LinkStoreJanusGraph(Properties props) throws IOException, Exception {
    super();
    initialize(props, Phase.LOAD, 0);
  }

  public long getNodeID() {
    try {
      lock.lock();
      long res = nodeIdIncrementer;
      nodeIdIncrementer++;
      return res;
    } finally {
      lock.unlock();
    }
  }

  public void resetNodeIDIncrementer(long startID) {
    try {
      lock.lock();
      nodeIdIncrementer = startID;
    } finally {
      lock.unlock();
    }
  }

  public String graphValueConverter(int val) {
    return String.valueOf(val);
  }


  public String graphValueConverter(long val) {
    return String.valueOf(val);
  }

  public String graphValueConverter(byte val) {
    return String.valueOf(val);
  }

  private String hexStringLiteral(byte[] arr) {
    StringBuilder sb = new StringBuilder();
    sb.append("x'");
    for (int i = 0; i < arr.length; i++) {
      byte b = arr[i];
      int lo = b & 0xf;
      int hi = (b >> 4) & 0xf;
      sb.append(Character.forDigit(hi, 16));
      sb.append(Character.forDigit(lo, 16));
    }
    sb.append("'");
    return sb.toString();
  }
  public String graphValueConverter(byte[] arr) {
    CharBuffer cb = StandardCharsets.ISO_8859_1.decode(ByteBuffer.wrap(arr));
    StringBuilder sb = new StringBuilder();
    sb.append('\'');
    for (int i = 0; i < cb.length(); i++) {
      char c = cb.get(i);
      switch (c) {
        case '\'':
          sb.append("\\'");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\0':
          sb.append("\\0");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (Character.getNumericValue(c) < 0) {
            // Fall back on hex string for values not defined in latin-1
            return hexStringLiteral(arr);
          } else {
            sb.append(c);
          }
      }
    }
    sb.append('\'');
    return sb.toString();
  }

  public void initialize(Properties props, Phase currentPhase,
                         int threadId) throws IOException, Exception {
    // connect
    try {

      graphConfigFilename = ConfigUtil.getPropertyRequired(props, CONFIG_FILENAME);
      openConnection();
    } catch (Exception e) {
      logger.error("error connecting to JanusGraph:", e);
      throw e;
    }
  }

  private void openConnection() throws Exception {
    g = traversal().withRemote(graphConfigFilename);
  }

  @Override
  public void close() {
    try {
      g.close();
    } catch (Exception e) {
      logger.error("Error while closing JanusGraph connection: ", e);
    }
  }

  public void clearErrors(int threadID) {
    logger.info("Reopening JanusGraph connection in threadID " + threadID);

    try {
      openConnection();
    } catch (Throwable e) {
      e.printStackTrace();
      return;
    }
  }

  @Override
  public boolean addLink(String dbid, Link l, boolean noinverse)
          throws Exception {
    while (true) {
      try {
        return addLinkImpl(l, noinverse);
      } catch (Exception ex) {
        throw ex;
//        if (!processSQLException(ex, "addLink")) {
//          throw ex;
//        }
      }
    }
  }

  private boolean addLinkImpl(Link l, boolean noinverse) throws Exception {
//    if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
//      logger.debug("addLink " + l.id1 +
//              "." + l.id2 +
//              "." + l.link_type);
//    }

    int numOfExistingLinks = addLinksInGraph(Collections.singletonList(l));
//    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
//      logger.trace("add/update link: " + l.toString());
//    }

    return numOfExistingLinks == 1;
  }

  @Override
  public boolean deleteLink(String dbid, long id1, long link_type, long id2,
                            boolean noinverse, boolean expunge)
          throws Exception {
    while (true) {
      try {
        return deleteLinkImpl(dbid, id1, link_type, id2, noinverse, expunge);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private boolean deleteLinkImpl(String dbid, long id1, long link_type, long id2,
      boolean noinverse, boolean expunge) throws Exception {
//    if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
//      logger.debug("deleteLink " + id1 +
//              "." + id2 +
//              "." + link_type);
//    }

    // check if link exists
    GraphTraversal gt = null;
    Edge e = getGraphTraversalForLinkInGraph(id1, id2, link_type);

    boolean found = e != null;
    if (!found) {
      // do nothing
    } else {

      RelationIdentifier rid = (RelationIdentifier) e.id();

      gt = g.E().hasId(rid);
      // check its visibility
      byte visibilityVal = (byte) gt.values(Link.VISIBILITY).next();
      if (visibilityVal != VISIBILITY_DEFAULT && !expunge) {
        // do nothing
      } else {
        gt = g.E().hasId(rid);
        if (!expunge) {
          // update the edge, set it as invisible
          gt.property(Link.VISIBILITY, VISIBILITY_HIDDEN);
          gt.next();
        } else {
          // delete the edge
          gt.drop().iterate();
        }
        g.tx().commit();
      }
    }
    return found;
  }

  @Override
  public boolean updateLink(String dbid, Link l, boolean noinverse)
          throws Exception {
    // Retry logic is in addLink
    boolean added = addLink(dbid, l, noinverse);
    return !added; // return true if updated instead of added
  }

  @Override
  public Link getLink(String dbid, long id1, long link_type, long id2)
          throws Exception {
    while (true) {
      try {
        return getLinkImpl(dbid, id1, link_type, id2);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private Link getLinkImpl(String dbid, long id1, long link_type, long id2)
          throws Exception {
    Link[] res = multigetLinks(dbid, id1, link_type, new long[] {id2});
    if (res == null) return null;
    assert(res.length <= 1);
    return res.length == 0 ? null : res[0];
  }

  @Override
  public Link[] multigetLinks(String dbid, long id1, long link_type,
                              long[] id2s) throws Exception {
    while (true) {
      try {
        return multigetLinksImpl(dbid, id1, link_type, id2s);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private Link[] multigetLinksImpl(String dbid, long id1, long link_type,
                                   long[] id2s) throws Exception {
    GraphTraversal gt = getGraphTraversalForLinksFromGivenSourceDst(id1, id2s, link_type);
    List<Edge> results = gt.toList();

    Link[] links = new Link[results.size()];
    for (int i = 0; i<results.size(); i++) {
      // construct Links;
      Edge e = results.get(i);
      long e_id1 = (long) e.values(Link.ID1).next();
      long e_id2 = (long) e.values(Link.ID2).next();
      byte e_visibility = (byte) e.values(Link.VISIBILITY).next();
      long e_link_type = (long) e.values(Link.LINK_TYPE).next();
      int e_version = (int) e.values(Link.VERSION).next();
      long e_time = (long) e.values(Link.TIME).next();
      String e_data = (String) e.values(Link.DATA).next();

      Link l = new Link(e_id1, e_link_type, e_id2, e_visibility, e_data.getBytes(), e_version, e_time);
      links[i] = l;
    }

    return links;
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type)
          throws Exception {
    // Retry logic in getLinkList
    return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
  }

  @Override
  public Link[] getLinkList(String dbid, long id1, long link_type,
                            long minTimestamp, long maxTimestamp,
                            int offset, int limit)
          throws Exception {
    while (true) {
      try {
        return getLinkListImpl(dbid, id1, link_type, minTimestamp,
                maxTimestamp, offset, limit);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private Link[] getLinkListImpl(String dbid, long id1, long link_type,
                                 long minTimestamp, long maxTimestamp,
                                 int offset, int limit) {
    GraphTraversal gt = getGraphTraversalForLinksFromGivenSource(id1, link_type);
    List<Edge> results = gt.toList();

    int startIndex = offset;
    int endIndex = Integer.min(offset + limit, results.size()) ;
    if (startIndex >= endIndex) {
      return null;
    }

    Link[] links = new Link[endIndex - startIndex];

    for (int i = startIndex; i < endIndex; i++) {
      // construct Links;
      Edge e = results.get(i);
      long e_id1 = (long) e.values(Link.ID1).next();
      long e_id2 = (long) e.values(Link.ID2).next();
      byte e_visibility = (byte) e.values(Link.VISIBILITY).next();
      long e_link_type = (long) e.values(Link.LINK_TYPE).next();
      int e_version = (int) e.values(Link.VERSION).next();
      long e_time = (long) e.values(Link.TIME).next();
      String e_data = (String) e.values(Link.DATA).next();

      Link l = new Link(e_id1, e_link_type, e_id2, e_visibility, e_data.getBytes(), e_version, e_time);
      links[i] = l;
    }

    return links;
  }

  @Override
  public long countLinks(String dbid, long id1, long link_type)
          throws Exception {
    while (true) {
      try {
        return countLinksImpl(dbid, id1, link_type);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private long countLinksImpl(String dbid, long id1, long link_type)
          throws Exception {
    GraphTraversal gt = getGraphTraversalForLinksFromGivenSource(id1, link_type);
    // only count those that are visible
    long count = (long) gt.has(Link.VISIBILITY, VISIBILITY_DEFAULT).count().next();
    return count;
  }

  @Override
  public int bulkLoadBatchSize() {
    return bulkInsertSize;
  }

  @Override
  public void addBulkLinks(String dbid, List<Link> links, boolean noinverse)
          throws Exception {
    while (true) {
      try {
        addBulkLinksImpl(dbid, links, noinverse);
        return;
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private void addBulkLinksImpl(String dbid, List<Link> links, boolean noinverse)
          throws Exception {
//    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
//      logger.trace("addBulkLinks: " + links.size() + " links");
//    }
    addLinksInGraph(links);
  }

  @Override
  public void addBulkCounts(String dbid, List<LinkCount> counts)
          throws Exception {
    while (true) {
      try {
        addBulkCountsImpl(dbid, counts);
        return;
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private void addBulkCountsImpl(String dbid, List<LinkCount> counts)
          throws Exception {
    // do nothing
  }

  @Override
  public void resetNodeStore(String dbid, long startID) throws Exception {
    dropAllNodes();
    resetNodeIDIncrementer(startID);
  }


  @Override
  public long addNode(String dbid, Node node) throws Exception {
    while (true) {
      try {
        return addNodeImpl(dbid, node);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  @Override
  public long[] bulkAddNodes(String dbid, List<Node> nodes) throws Exception {
    while (true) {
      try {
        return bulkAddNodesImpl(dbid, nodes);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private long addNodeImpl(String dbid, Node node) throws Exception {
    long ids[] = bulkAddNodes(dbid, Collections.singletonList(node));
    assert(ids.length == 1);
    return ids[0];
  }

  private long[] bulkAddNodesImpl(String dbid, List<Node> nodes) throws Exception {
    return addNodesInGraph(nodes);
  }

  /**
   * Graph Internal Operations: insert nodes in the graph
   */
  private long[] addNodesInGraph(List<Node> nodes) {
    GraphTraversal gt = null;

    long[] newNodeIDs = new long[nodes.size()];
    for (int i = 0; i<nodes.size(); i++) {
      Node node = nodes.get(i);


      if (gt == null) {
        gt = g.addV(nodeLabel);
      } else {
        gt.addV(nodeLabel);
      }

      long nid = getNodeID();
      newNodeIDs[i] = nid;

      gt.property(Node.ID, nid);
      gt.property(Node.TYPE, node.type);
      gt.property(Node.VERSION, node.version);
      gt.property(Node.TIME, node.time);
      gt.property(Node.DATA, graphValueConverter(node.data));
    }

    gt.next();
    g.tx().commit();
    return newNodeIDs;
  }

  @Override
  public Node getNode(String dbid, int type, long id) throws Exception {
    while (true) {
      try {
        return getNodeImpl(dbid, type, id);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private Node getNodeImpl(String dbid, int type, long id) throws Exception {
    Node res = getNodeInGraph(id);

    if (res != null && res.type == type) {
      return res;
    }

    return null;
  }

  @Override
  public boolean updateNode(String dbid, Node node) throws Exception {
    while (true) {
      try {
        return updateNodeImpl(dbid, node);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private boolean updateNodeImpl(String dbid, Node node) throws Exception {
    return updateNodeInGraph(node.id, node);
  }

  @Override
  public boolean deleteNode(String dbid, int type, long id) throws Exception {
    while (true) {
      try {
        return deleteNodeImpl(dbid, type, id);
      } catch (Exception ex) {
        throw ex;
      }
    }
  }

  private boolean deleteNodeImpl(String dbid, int type, long id) throws Exception {
    return deleteNodeInGraph(id, type);
  }

  public class GraphNode {
    public boolean isGraphNodeExists = false;
    public long graphNodeID;

    public GraphNode(boolean isExists, long gid) {
      isGraphNodeExists = isExists;
      graphNodeID = gid;
    }
  }

  /**
   * Graph Internal Operations: get actual node id in graph by its given ID
   */
  private Vertex getGraphVertexInGraph(long id) {
    Optional<Vertex> gt = g.V().hasLabel(nodeLabel).has(Node.ID, id).tryNext();
    return gt.orElse(null);
  }

  /**
   * Graph Internal Operations: update node by its given ID
   */
  private boolean deleteNodeInGraph(long id, int type) {
    Vertex vtx = getGraphVertexInGraph(id);
    if (vtx == null) {
      return false;
    }

    long graphNodeID = (long) vtx.id();
    Map<Object, Object> nodeValMap = g.V(graphNodeID).valueMap().next();
    ArrayList<Integer> nodeTypeArr = (ArrayList<Integer>) nodeValMap.get(Node.TYPE);

    int nodeType = nodeTypeArr.get(0);
    if (nodeType != type) {
      return false;
    }

    g.V(graphNodeID).drop().iterate();
    g.tx().commit();
    return true;
  }

  /**
   * Graph Internal Operations: update node by its given ID
   */
  private boolean updateNodeInGraph(long id, Node newNode) {
    Vertex vtx = getGraphVertexInGraph(id);
    if (vtx == null) {
      return false;
    }

    long graphNodeID = (long) vtx.id();
    GraphTraversal gt = g.V(graphNodeID);

    gt.property(Node.VERSION, newNode.version);
    gt.property(Node.TIME, newNode.time);
    gt.property(Node.DATA, graphValueConverter(newNode.data));

    gt.next();
    g.tx().commit();
    return true;
  }


  /**
   * Graph Internal Operations: get node by its given ID
   */
  private Node getNodeInGraph(long id) {
    Vertex vtx = getGraphVertexInGraph(id);
    if (vtx == null) {
      return null;
    }

    long graphNodeID = (long) vtx.id();
    Map<Object, Object> nodeValMap = g.V(graphNodeID).valueMap().next();

    ArrayList<Integer> nodeType = (ArrayList<Integer>) nodeValMap.get(Node.TYPE);
    ArrayList<Long> nodeVersion = (ArrayList<Long>) nodeValMap.get(Node.VERSION);
    ArrayList<Long> nodeTime = (ArrayList<Long>) nodeValMap.get(Node.TIME);
    ArrayList<String> nodeData = (ArrayList<String>) nodeValMap.get(Node.DATA);

    Node node = new Node(id, nodeType.get(0), nodeVersion.get(0), nodeTime.get(0), nodeData.get(0).getBytes());
    return node;
  }

  /**
   * Graph Internal Operations: drop all vertices
   */
  private void dropAllNodes() {
    g.V().drop().iterate();
    g.tx().commit();
  }

  /**
   * Graph Internal Operations: add/update links in the graph
   * @param links
   * @return
   */
  private int addLinksInGraph(List<Link> links) {
    int numOfExistingLink = 0;
    if (links.size() == 0) {
      return numOfExistingLink;
    }
    List<GraphTraversal> gtArr = new ArrayList<>();

    for (int i = 0; i<links.size(); i++) {
      Link l = links.get(i);

      long id1 = l.id1;
      long id2 = l.id2;

      Edge e = getGraphTraversalForLinkInGraph(l.id1, l.id2, l.link_type);

      GraphTraversal gt = null;
      if (e != null) {
        // the link exists, update properties
        RelationIdentifier rid = (RelationIdentifier) e.id();

//        gt = g.V(rid.getOutVertexId()).outE(linkLabel).where(__.hasId(rid.getInVertexId()));
        gt = g.E().hasId(rid);
        gt.property(Link.TIME, l.time);
        gt.property(Link.VERSION, l.version);
        gt.property(Link.DATA, graphValueConverter(l.data));

        gtArr.add(gt);
        numOfExistingLink++;
      } else {
        gt = g.V().has(Node.ID, id1);
        // find source
        String stepLabel1 = "id1"+ i;
        gt.as(stepLabel1);

        gt.V().has(Node.ID, id2);
        String stepLabel2 = "id2"+i;
        gt.as(stepLabel2);

        gt.addE(linkLabel);
        gt.property(Link.ID1, id1);
        gt.property(Link.ID2, id2);
        gt.property(Link.LINK_TYPE, l.link_type);
        gt.property(Link.VISIBILITY, l.visibility);
        gt.property(Link.DATA, graphValueConverter(l.data));
        gt.property(Link.VERSION, l.version);
        gt.property(Link.TIME, l.time);

        gt.from(stepLabel1).to(stepLabel2);

        gtArr.add(gt);
      }
    }

    for (GraphTraversal gt : gtArr) {
      gt.next();
    }
    g.tx().commit();

    return numOfExistingLink;
  }

  /**
   * Graph Internal Operations: check if given link exists in the graph
   */
  private Edge getGraphTraversalForLinkInGraph(long id1, long id2, long link_type) {
    GraphTraversal gt = null;

    gt = g.V().has(Node.ID, id1)
            .outE(linkLabel).has(Link.LINK_TYPE, graphValueConverter(link_type))
            .where(__.inV().hasLabel(nodeLabel).has(Node.ID, id2));
    Optional<Edge> e = gt.tryNext();
    return e.orElse(null);
  }

  /**
   * Graph Internal Operations: check if given link exists in the graph
   */
  private GraphTraversal getGraphTraversalForLinksFromGivenSourceDst(long id1, long[] id2, long link_type) {

    GraphTraversal gt = null;

    gt = g.V().has(Node.ID, id1)
            .outE(linkLabel).has(Link.LINK_TYPE, graphValueConverter(link_type))
            .where(__.inV().hasLabel(nodeLabel).has(Node.ID, P.within(id2)));
    return gt;
  }

  /**
   * Graph Internal Operations: check if given link exists in the graph
   */
  private GraphTraversal getGraphTraversalForLinksFromGivenSource(long id1, long link_type) {

    GraphTraversal gt = null;

    gt = g.V().has(Node.ID, id1)
            .outE(linkLabel).has(Link.LINK_TYPE, graphValueConverter(link_type));
    return gt;
  }

}
