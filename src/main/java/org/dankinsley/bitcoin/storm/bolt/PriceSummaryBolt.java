package org.dankinsley.bitcoin.storm.bolt;

import java.io.IOException;
import java.util.Map;

import org.dankinsley.bitcoin.storm.util.SendEmail;
import org.dankinsley.bitcoin.storm.util.TickerStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PriceSummaryBolt extends BaseRichBolt {
	private static final long serialVersionUID = -4926674399804657333L;
	private static final Logger logger =  LoggerFactory.getLogger(PriceSummaryBolt.class.getName());
	private OutputCollector collector;
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
    	String metric = tuple.getStringByField("metric");
		TickerStatistics stats = (TickerStatistics) tuple.getValueByField("statistics");
    	
    	// logger.info(metric + " :\n"+ stats.toString());
    	Double delta = Math.abs(stats.getMax() - stats.getMin());
    	if(metric.equals("Bitstamp-last") && delta > 5){
    		try {
    			SendEmail.sendAlert("Price alert: " + metric + " ~ $" + delta);
    		} catch (IOException e) {
    			logger.info("!!!!!!!!!! Price alert: " + metric + " ~ $" + delta );
    		}
    	}
    	else {
    		logger.info("(Not a) Price alert: " + metric + " ~ $" + delta);
    		logger.info("Tuple: "+ metric + " : " + stats.toString());
    	}
    	
    	collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds"));
    }

}
