package controllers;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;

import org.apache.storm.kafka.*;
import storm.kafka.*;

public class StormController {

    public static void main(String[] args) throws Exception{
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        BrokerHosts hosts = new ZkHosts("localhost:9092");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, "test", "localhost:2181", "id1");
        spoutConfig.scheme = new org.apache.storm.spout.MultiScheme((Scheme) new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("line-reader-spout", (IRichSpout) kafkaSpout);



        LocalCluster cluster = new LocalCluster();
        System.out.println("submit topology");
        Thread.sleep(10000);
        //StormSubmitter.submitTopology("HelloStorm5", config, builder.createTopology());
        cluster.submitTopology("HelloStorm5", config, builder.createTopology());
        cluster.shutdown();
    }

}
