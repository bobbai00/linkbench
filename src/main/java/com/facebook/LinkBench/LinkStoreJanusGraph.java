package com.facebook.LinkBench;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class LinkStoreJanusGraph extends GraphStore {

  public static final String CONFIG_FILENAME = "graphConfigFilename";

  String nodeLabel = "node";
  String linkLabel = "link";

  String graphConfigFilename;

  GraphTraversalSource g;


  Level debuglevel;

  private Phase phase;

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

      String id1 = graphValueConverter(l.id1);
      String id2 = graphValueConverter(l.id2);

      gt = g.V().has(Node.ID, id1)
              .outE(linkLabel).has(Link.LINK_TYPE, graphValueConverter(l.link_type))
                      .where(__.inV().hasLabel(nodeLabel).has(Node.ID, id2));

      if (gt.hasNext()) {
        // the link exists, update properties
        gt.property(Link.TIME, graphValueConverter(l.time));
        gt.property(Link.VERSION, graphValueConverter(l.version));
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
        gt.property(Link.LINK_TYPE, graphValueConverter(l.link_type));
        gt.property(Link.VISIBILITY, graphValueConverter(l.visibility));
        gt.property(Link.DATA, graphValueConverter(l.data));
        gt.property(Link.VERSION, graphValueConverter(l.version));
        gt.property(Link.TIME, graphValueConverter(l.time));

        gt.from(stepLabel1).to(stepLabel2);
      }
    }
    gt.next();
    g.tx().commit();

    return numOfExistingLink;
  }

}
