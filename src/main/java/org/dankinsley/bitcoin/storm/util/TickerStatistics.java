package org.dankinsley.bitcoin.storm.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TickerStatistics {
	private static final Logger logger =  LoggerFactory.getLogger(TickerStatistics.class.getName());
	private Double min;
	private Double max;
	private Double avg;
	private Double sum = new Double(0);
	private Double sumVarianceSq = new Double(0);
	private Double volitility;
	private int count = 0;
	
	
	public void setMax(Double max){
		this.max = max;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	public void incrementCount(){
		count++;
	}

	public Double getMin() {
		return min;
	}

	public Double getMax() {
		return max;
	}

	public void setMin(Double min) {
		this.min = min;
	}

	public Double getAvg() {
		return avg;
	}

	public void setAvg(Double avg) {
		this.avg = avg;
	}

	public Double getSum() {
		return sum;
	}

	public void setSum(Double sum) {
		this.sum = sum;
	}
	
	public void addValue(Double value){
		sum += value;
		count++;
		if(count == 1 || value > getMax()){
        	setMax(value);
        }
        if(count == 1 || value < getMin()){
        	setMin(value);
        }
        setAvg(sum / count);
        
        sumVarianceSq += Math.pow((value - getAvg()),2);
        setVolitility(sumVarianceSq / count);
        
	}
	public void mergeStatistics(TickerStatistics input){
		sum += input.getSum();
		count += input.getCount();
		sumVarianceSq += input.getSumVarianceSq();
		if(getMax() == null || input.getMax() > getMax()){
        	setMax(input.getMax());
        }
        if(getMin() == null ||  input.getMin() < getMin()){
        	setMin(input.getMin());
        }
        setAvg(sum / count);
        
        setVolitility(sumVarianceSq / count);		
	}

	public Double getSumVarianceSq() {
		return sumVarianceSq;
	}

	public Double getVolitility() {
		return volitility;
	}

	public void setVolitility(Double volitility) {
		this.volitility = volitility;
	}
	
	public String toString(){
		logger.debug("Outputting ticker statistics");
		return "Count: " + count + "\n" +
				"Max: " + max + "\n" +
				"Min: " + min + "\n" +
				"Avg: " + avg + "\n" +
				"Volitility: " + volitility + "\n"
				;
	}

	public void setSumVarianceSq(Double sumVarianceSq) {
		this.sumVarianceSq = sumVarianceSq;
	}

}
