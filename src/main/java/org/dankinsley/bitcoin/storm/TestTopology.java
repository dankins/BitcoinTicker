package org.dankinsley.bitcoin.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import org.dankinsley.bitcoin.storm.bolt.PriceSummaryBolt;
import org.dankinsley.bitcoin.storm.bolt.RollingPriceBolt;
import org.dankinsley.bitcoin.storm.spout.PollingBitcoinTickerSpout;

/**
 * This topology connects to MtGox and Bitstamp API and will send an email if the Last price changes more than $5 in an hour
 */
public class TestTopology {
    
    public static void main(String[] args) throws Exception {
 
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("bitstampSpout",  new PollingBitcoinTickerSpout("Bitstamp"), 1);
        builder.setSpout("mtgoxSpout",  new PollingBitcoinTickerSpout("MtGox"), 1);
        builder.setBolt("pricecounter", new RollingPriceBolt(600, 30), 1)
        	.fieldsGrouping("bitstampSpout", new Fields("exchange"))
        	.fieldsGrouping("mtgoxSpout", new Fields("exchange"));
        builder.setBolt("summary", new PriceSummaryBolt(), 1)
        	.shuffleGrouping("pricecounter");
        
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(1);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(12400000);
        cluster.killTopology("test");
        cluster.shutdown();
         
         
    }
}