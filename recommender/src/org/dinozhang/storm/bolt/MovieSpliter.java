package org.dinozhang.storm.bolt;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MovieSpliter extends BaseBasicBolt {

    private static final long serialVersionUID = -5653803832498574866L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        List<Object> list = input.getValues();

        String id = (String) list.get(0);
        String name = (String) list.get(1);
        String published_year = (String) list.get(2);
        String type = (String) list.get(3);

        collector.emit(new Values(id, name, type, published_year));
        /*String data = input.getString(0);
        //id, name, type, published_year
        if(data!=null && data.length()>0){
        	String[] values = data.split("::");
        	if(values.length==4){
        		String id = values[0];
        		String name = values[1];
        		String type = values[2];
        		String published_year = values[3];
        		collector.emit(new Values(id, name, type, published_year));
        	 }
        }*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "name", "type", "published_year"));

    }

}
