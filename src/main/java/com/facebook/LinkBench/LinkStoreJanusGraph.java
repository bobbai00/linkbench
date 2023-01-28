package com.facebook.LinkBench;

import org.apache.commons.configuration2.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class LinkStoreJanusGraph extends GraphStore {

  public static final String CONFIG_FILENAME = "graphConfigFilename";

  String linkLabel;
  String edgeLabel;

  String graphConfigFilename;

  GraphTraversalSource graphTraversalSource;

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
    graphTraversalSource = traversal().withRemote(graphConfigFilename);
  }

  @Override
  public void close() {
    try {
      graphTraversalSource.close();
      graphTraversalSource.V().next();
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
}
