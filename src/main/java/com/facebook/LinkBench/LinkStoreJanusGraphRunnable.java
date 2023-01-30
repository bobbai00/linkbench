package com.facebook.LinkBench;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class LinkStoreJanusGraphRunnable {


    public static void testAddNodes(GraphStore graphStore) throws Exception {
        String data = "123456";
        String dbID = "1";

        graphStore.resetNodeStore(dbID, 1);


        List<Node> nodeList = new ArrayList<>();

        nodeList.add(new Node(1, 1, 1, System.currentTimeMillis(), data.getBytes()));
        nodeList.add(new Node(2, 2, 1, System.currentTimeMillis(), data.getBytes()));
        nodeList.add(new Node(3, 3, 1, System.currentTimeMillis(), data.getBytes()));

        long[] ids = graphStore.bulkAddNodes(dbID, nodeList);

        Node node1 = graphStore.getNode(dbID, 1, 1);
        Node node2 = graphStore.getNode(dbID, 2, 2);
        Node node3 = graphStore.getNode(dbID, 3, 3);

        graphStore.deleteNode(dbID,  2, 2);

        Node node2_missing = graphStore.getNode(dbID, 2, 2);

        graphStore.updateNode(dbID, new Node(1, 1, 10000, System.currentTimeMillis(), "654321".getBytes()));
        Node node1_new = graphStore.getNode(dbID, 1, 1);

        graphStore.resetNodeStore(dbID, 10);
    }

    public static void testLinks(GraphStore graphStore) throws Exception {
        String data = "123456";
        String dbID = "1";

        graphStore.resetNodeStore(dbID, 1);

        List<Node> nodeList = new ArrayList<>();

        nodeList.add(new Node(1, 1, 1, System.currentTimeMillis(), data.getBytes()));
        nodeList.add(new Node(2, 2, 1, System.currentTimeMillis(), data.getBytes()));
        nodeList.add(new Node(3, 3, 1, System.currentTimeMillis(), data.getBytes()));

        long[] ids = graphStore.bulkAddNodes(dbID, nodeList);

        // add two new links
        List<Link> linkList = new ArrayList<>();
        linkList.add(new Link(1L, 1L, 3L, (byte) 1, data.getBytes(), 1, System.currentTimeMillis()));
        linkList.add(new Link(2L, 1L, 3L, (byte) 1, data.getBytes(), 1, System.currentTimeMillis()));

        graphStore.addBulkLinks(dbID, linkList, false);

        // modify existing links
        boolean isUpdateSucceed = graphStore.addLink(dbID, new Link(1L, 1L, 3L, (byte) 0, data.getBytes(), 10000, System.currentTimeMillis()), false);


        graphStore.resetNodeStore(dbID, 1);

    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("graphConfigFilename", "config/remote-graph.properties");


        try {
            GraphStore graphStore = new LinkStoreJanusGraph(props);
            testLinks(graphStore);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }


    }


}
