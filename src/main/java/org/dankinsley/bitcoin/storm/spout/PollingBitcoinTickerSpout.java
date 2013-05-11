package org.dankinsley.bitcoin.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xeiam.xchange.Exchange;
import com.xeiam.xchange.ExchangeFactory;
import com.xeiam.xchange.currency.Currencies;
import com.xeiam.xchange.dto.marketdata.Ticker;
import com.xeiam.xchange.service.marketdata.polling.PollingMarketDataService;
import com.xeiam.xchange.bitstamp.BitstampExchange;
import com.xeiam.xchange.mtgox.v1.MtGoxExchange;

public class PollingBitcoinTickerSpout extends BaseRichSpout {
	private static final long serialVersionUID = -2867527317524150260L;
	SpoutOutputCollector _collector;
	static final Logger logger = LoggerFactory.getLogger(PollingBitcoinTickerSpout.class.getName());
	PollingMarketDataService marketDataService;
	String exchangeName = "unset";

	public PollingBitcoinTickerSpout(String exchangeName){
		this.exchangeName = exchangeName;
	}
	
    @SuppressWarnings("rawtypes")
	@Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        
        if(exchangeName.equals("Bitstamp")){
        	Exchange exchange = ExchangeFactory.INSTANCE.createExchange(BitstampExchange.class.getName());
            marketDataService = exchange.getPollingMarketDataService();
        }
        else if(exchangeName.equals("MtGox")){
        	Exchange exchange = ExchangeFactory.INSTANCE.createExchange(MtGoxExchange.class.getName());
            marketDataService = exchange.getPollingMarketDataService();
        }
        else {
        	Exchange exchange = ExchangeFactory.INSTANCE.createExchange(MtGoxExchange.class.getName());
            marketDataService = exchange.getPollingMarketDataService();
        }

    }

    @Override
    public void nextTuple() {
        
        Ticker ticker = marketDataService.getTicker(Currencies.BTC, Currencies.USD);
        logger.info(exchangeName + " - " + ticker.toString());
        
        Double last = ticker.getLast().getAmount().doubleValue();
        Double bid = ticker.getBid().getAmount().doubleValue();
        Double ask = ticker.getAsk().getAmount().doubleValue();
        Double volume = ticker.getVolume().doubleValue();
        _collector.emit(new Values(exchangeName,last,bid,ask,volume));

        // Sleep for 10 seconds so we don't DDOS the API
        Utils.sleep(10000);
    }        


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("exchange","last","bid","ask","volume"));
    }
    
}