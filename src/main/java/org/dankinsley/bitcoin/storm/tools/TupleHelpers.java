package org.dankinsley.bitcoin.storm.tools;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;
/**
 * NOTE: THIS CODE IS COPIED FROM THE STORM STARTER PROJECT FOUND HERE:
 * https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/util/TupleHelpers.java
 * 
 */
public final class TupleHelpers {

    private TupleHelpers() {
    }

    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

}