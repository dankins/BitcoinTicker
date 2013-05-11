package org.dankinsley.bitcoin.storm.tools;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dankinsley.bitcoin.storm.util.TickerStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NOTE: THIS CODE IS ADAPTED FROM THE STORM STARTER PROJECT FOUND HERE:
 * https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/tools/SlotBasedCounter.java
 * THe original code only tracks the "count", while this code tracks min/max/avg/sum/volatility
 * 
 * This class provides per-slot counts of the occurrences of objects.
 * 
 * It can be used, for instance, as a building block for implementing sliding window counting of objects.
 * 
 * @param <T>
 *            The type of those objects we want to count.
 */
public final class SlotBasedStatistics<T> implements Serializable {

    private static final long serialVersionUID = 4858185737378394432L;
    private static final Logger logger =  LoggerFactory.getLogger(SlotBasedStatistics.class.getName());

    private final Map<String, TickerStatistics[]> exchangeStatistics = new HashMap<String, TickerStatistics[]>();
    private final int numSlots;

    public SlotBasedStatistics(int numSlots) {
        if (numSlots <= 0) {
            throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots
                + ")");
        }
        this.numSlots = numSlots;
    }

    public void updateStatistics(String metric, Double value, int slot) {
    	TickerStatistics[] statistics = exchangeStatistics.get(metric);
        if (statistics == null) {
        	statistics = new TickerStatistics[this.numSlots];
        	for(int i=0; i<this.numSlots;i++){
        		statistics[i] = new TickerStatistics();
        	}
        	exchangeStatistics.put(metric, statistics);
        }
        statistics[slot].addValue(value);
    }

    public TickerStatistics getStatistics(T obj, int slot) {
    	TickerStatistics[] metrics = exchangeStatistics.get(obj);
        if (metrics == null) {
            return new TickerStatistics();
        }
        else {
            return metrics[slot];
        }
    }

    public Map<String, TickerStatistics> getStatistics() {
        Map<String, TickerStatistics> result = new HashMap<String, TickerStatistics>();
        for (String metric : exchangeStatistics.keySet()) {
            result.put(metric, computeStatistics(metric));
        }
        return result;
    }

    private TickerStatistics computeStatistics(String metric) {
    	TickerStatistics[] curr = exchangeStatistics.get(metric);
    	TickerStatistics stats = new TickerStatistics();

    	logger.debug("Compute Statistics: "+metric+ " - "  + curr.length);
        for (TickerStatistics l : curr) {
        	if(l.getCount() > 0){
        		stats.mergeStatistics(l);
        	}
        }
        return stats;
    }

    /**
     * Reset the slot count of any tracked objects to zero for the given slot.
     * 
     * @param slot
     */
    public void wipeSlot(int slot) {
        for (String exchange : exchangeStatistics.keySet()) {
            resetSlotCountToZero(exchange, slot);
        }
    }

    private void resetSlotCountToZero(String exchange, int slot) {
    	TickerStatistics[] stat = exchangeStatistics.get(exchange);
    	stat[slot] = new TickerStatistics();
    }

    private boolean shouldBeRemovedFromCounter(String exchange) {
        return computeStatistics(exchange).getCount() == 0;
    }

    /**
     * Remove any object from the counter whose total count is zero (to free up memory).
     */
    public void wipeZeros() {
        Set<String> objToBeRemoved = new HashSet<String>();
        for (String exchange : exchangeStatistics.keySet()) {
            if (shouldBeRemovedFromCounter(exchange)) {
                objToBeRemoved.add(exchange);
            }
        }
        for (String obj : objToBeRemoved) {
        	exchangeStatistics.remove(obj);
        }
    }

}