package com.facebook.LinkBench;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;

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

  private final Logger logger = Logger.getLogger(ConfigUtil.LINKBENCH_LOGGER);

  public LinkStoreJanusGraph() {
    super();
  }

  public LinkStoreJanusGraph(Properties props) throws IOException, Exception {
    super();
    initialize(props, Phase.LOAD, 0);
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
      g.V().next();
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
//        if (!processSQLException(ex, "addLink")) {
//          throw ex;
//        }
      }
    }
  }

  private boolean addLinkImpl(Link l, boolean noinverse) throws Exception {
    if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
      logger.debug("addLink " + l.id1 +
              "." + l.id2 +
              "." + l.link_type);
    }

    int numOfExistingLinks = addLinksInGraph(Collections.singletonList(l));
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("add/update link: " + l.toString());
    }

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
    if (Level.DEBUG.isGreaterOrEqual(debuglevel)) {
      logger.debug("deleteLink " + id1 +
              "." + id2 +
              "." + link_type);
    }

    // check if link exists
    GraphTraversal gt = null;
    gt = getGraphTraversalForLinkInGraph(id1, id2, link_type);

    boolean found = gt.hasNext();
    if (!found) {
      // do nothing
    } else {
      // check its visibility
      String visibilityVal = (String) gt.values(Link.VISIBILITY).next();
      if (!Link.checkVisibility(visibilityVal) && !expunge) {
        // do nothing
      } else {
        if (!expunge) {
          // update the edge, set it as invisible
          gt.property(Link.VISIBILITY, VISIBILITY_HIDDEN);
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
    if (Level.TRACE.isGreaterOrEqual(debuglevel)) {
      logger.trace("addBulkLinks: " + links.size() + " links");
    }

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
    GraphTraversal gt = null;

    for (int i = 0; i<links.size(); i++) {
      Link l = links.get(i);

      long id1 = l.id1;
      long id2 = l.id2;

      gt = getGraphTraversalForLinkInGraph(l.id1, l.id2, l.link_type);

      if (gt.hasNext()) {
        // the link exists, update properties
        gt.property(Link.TIME, l.time);
        gt.property(Link.VERSION, l.version);
        gt.property(Link.DATA, graphValueConverter(l.data));

        numOfExistingLink++;
      } else {
        gt.V();
        // find source
        gt.hasLabel(nodeLabel).has(Node.ID, id1);
        String stepLabel1 = "id1_"+i;
        gt.as("id1_"+i);

        gt.hasLabel(nodeLabel).has(Node.ID, id2);
        String stepLabel2 = "id2_"+i;
        gt.as(stepLabel2);

        gt.addE(linkLabel);
        gt.property(Link.ID1, id1);
        gt.property(Link.ID2, id2);
        gt.property(Link.LINK_TYPE, l.link_type);
        gt.property(Link.VISIBILITY, l.visibility);
        gt.property(Link.DATA, l.data);
        gt.property(Link.VERSION, l.version);
        gt.property(Link.TIME, l.time);

        gt.from(stepLabel1).to(stepLabel2);
      }
    }
    gt.next();
    g.tx().commit();

    return numOfExistingLink;
  }

  /**
   * Graph Internal Operations: check if given link exists in the graph
   */
  private GraphTraversal getGraphTraversalForLinkInGraph(long id1, long id2, long link_type) {
    GraphTraversal gt = null;

    gt = g.V().has(Node.ID, id1)
            .outE(linkLabel).has(Link.LINK_TYPE, graphValueConverter(link_type))
            .where(__.inV().hasLabel(nodeLabel).has(Node.ID, id2));
    return gt;
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

  /**
   * Graph Internal Operations: remove the given link
   */
  private void removeLinkFromGraph(long id1, long id2, long link_type) {
    GraphTraversal gt = getGraphTraversalForLinkInGraph(id1, id2, link_type);
    gt.drop().iterate();
    g.tx().commit();
  }

}
