package org.dankinsley.bitcoin.storm.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dankinsley.bitcoin.storm.tools.NthLastModifiedTimeTracker;
import org.dankinsley.bitcoin.storm.tools.SlidingWindowStatistics;
import org.dankinsley.bitcoin.storm.tools.TupleHelpers;
import org.dankinsley.bitcoin.storm.util.TickerStatistics;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * NOTE: THIS CODE IS ADAPTED FROM THE STORM STARTER PROJECT FOUND HERE:
 * https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/bolt/RollingCountBolt.java
 * 
 * This bolt performs rolling counts of incoming objects, i.e. sliding window based counting.
 * 
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * 
 * The bolt emits a rolling count tuple per object, consisting of the object itself, its latest rolling count, and the
 * actual duration of the sliding window. The latter is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high system load. Note that the actual
 * window length is tracked and calculated for the window, and not individually for each object within a window.
 * 
 * Note: During the startup phase you will usually observe that the bolt warns you about the actual sliding window
 * length being smaller than the expected length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during startup (e.g. you will see this warning
 * during the first ~ five minutes of startup time if the window length is set to five minutes).
 * 
 */
public class RollingPriceBolt extends BaseRichBolt {

    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger logger =  LoggerFactory.getLogger(RollingPriceBolt.class.getName());
    private static final int NUM_WINDOW_CHUNKS = 5;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE = "Actual window length is %d seconds when it should be %d seconds"
        + " (you can safely ignore this warning during the startup phase)";

    private final SlidingWindowStatistics<Object> stats;
    private final int windowLengthInSeconds;
    private final int emitFrequencyInSeconds;
    private OutputCollector collector;
    private NthLastModifiedTimeTracker lastModifiedTracker;

    public RollingPriceBolt() {
        this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public RollingPriceBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        stats = new SlidingWindowStatistics<Object>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
            this.emitFrequencyInSeconds));
    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
        return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
            this.emitFrequencyInSeconds));
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleHelpers.isTickTuple(tuple)) {
            logger.info("Received tick tuple, triggering emit of current window counts");
            emitCurrentWindowAnalysis();
        }
        else {
            analyzeAndAck(tuple);
        }
    }

    private void emitCurrentWindowAnalysis() {
    	Map<String, TickerStatistics> output = stats.getStatsThenAdvanceWindow();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            logger.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }

        emit(output, actualWindowLengthInSeconds);
    }

    private void emit(Map<String, TickerStatistics> values, int actualWindowLengthInSeconds) {
        for (Entry<String, TickerStatistics> entry : values.entrySet()) {
            String metric = entry.getKey();
            TickerStatistics statistics = entry.getValue();
            collector.emit(new Values(metric, statistics, actualWindowLengthInSeconds));
        }
    }

    private void analyzeAndAck(Tuple tuple) {
    	
        stats.addValue(tuple.getStringByField("exchange")+"-bid", tuple.getDoubleByField("bid"));
        stats.addValue(tuple.getStringByField("exchange")+"-ask", tuple.getDoubleByField("ask"));
        stats.addValue(tuple.getStringByField("exchange")+"-last", tuple.getDoubleByField("last"));
        stats.addValue(tuple.getStringByField("exchange")+"-volume", tuple.getDoubleByField("volume"));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("metric", "statistics", "actualWindowLengthInSeconds"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}