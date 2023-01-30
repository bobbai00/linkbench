package com.facebook.LinkBench;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Properties;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class LinkStoreJanusGraphRunnable {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("graphConfigFilename", "config/remote-graph.properties");


        try {
            Graph graph = EmptyGraph.instance();
            GraphTraversalSource g = traversal().withRemote("config/remote-graph.properties");
            // GraphStore graphStore = new LinkStoreJanusGraph(props);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }


}
