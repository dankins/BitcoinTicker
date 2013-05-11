package org.dankinsley.bitcoin.storm.util;

public class Tick {
	private String exchange;
	private Double bid;
	private Double ask;
	private Double last;
	private Double volume;
	
	public Tick(String exchange,Double bid,Double ask,Double last,Double volume){
		setExchange(exchange);
		setBid(bid);
		setAsk(ask);
		setLast(last);
		setVolume(volume);
	}
	
	public Tick(){};
	

	public Double getVolume() {
		return volume;
	}

	public void setVolume(Double volume) {
		this.volume = volume;
	}

	public Double getLast() {
		return last;
	}

	public void setLast(Double last) {
		this.last = last;
	}

	public Double getBid() {
		return bid;
	}

	public void setBid(Double bid) {
		this.bid = bid;
	}

	public Double getAsk() {
		return ask;
	}

	public void setAsk(Double ask) {
		this.ask = ask;
	}

	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}
}
