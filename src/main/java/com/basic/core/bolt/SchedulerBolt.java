package com.basic.core.bolt;

import com.basic.core.Constraints;
import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.spark.util.sketch.CountMinSketch;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * locate com.basic.storm.bolt
 * Created by windy on 2019/1/2
 */
public class SchedulerBolt extends BaseRichBolt {
    private String UPSTREAM_COMPONENT_ID;
    private String UPSTREAM_FIELDS;
    private OutputCollector collector;
    private CountingBloomFilter bf;
    private CountingMinSketch cms;
    private static final List<String> SCHEMA = ImmutableList.of("relation", "timestamp", "key", "value");

    public SchedulerBolt(String UPSTREAM_COMPONENT_ID,String UPSTREAM_REL_FIELDS, UPSTREAM_KEY_FIELDS) {
        this.UPSTREAM_COMPONENT_ID = UPSTREAM_COMPONENT_ID;
        this.UPSTREAM_FIELDS=UPSTREAM_FIELDS;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.bf_R = new CountingBloomFilter(16,4,1);
        this.bf_S = new CountingBloomFilter(16,4,1);
	  this.cms_R = CountMinSketch.create(0.05, 0.05, 5);
	  this.cms_S = CountMinSketch.create(0.05, 0.05, 5);
    }

    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().equals(UPSTREAM_COMPONENT_ID)){
            String rel = tuple.getStringByField(UPSTREAM_REL_FIELDS);
            String word = tuple.getStringByField(UPSTREAM_KEY_FIELDS);
		
		Long ts = tuple.getStringByField("Constraints.tsFileds");
		String value = tuple.getStringByField("Constraints.valueSFileds");

            if(word.length() <= 0) {
                collector.ack(tuple);
                return;
            }

            Key ky = new Key(word.getBytes());

		if (rel.equals("R")) {
			cms_R.add(ky);
			Long baseCount = cms_R.estimateCount(ky);
          	  collector.emit(Constraints.coinFileds,new Values(rel,word,baseCount));
         	   if(bf_R.membershipTest(ky))
             	   	collector.emit(Constraints.hotRFileds, tuple, new Values(rel,ts,word,value));
           	   else
                	collector.emit(Constraints.nohotRFileds, tuple, new Values(rel,ts,word,value));
		}else{
			cms_S.add(ky);
			Long baseCount = cms_S.estimateCount(ky);
          	  collector.emit(Constraints.coinFileds,new Values(rel,word,baseCount));
         	   if(bf_S.membershipTest(ky))
             	   	collector.emit(Constraints.hotSFileds, tuple, new Values(rel,ts,word,value));
           	   else
                	collector.emit(Constraints.nohotSFileds, tuple, new Values(rel,ts,word,value));
		}

        }else {
            String rel = tuple.getStringByField(Constraints.relFileds);
            String key = tuple.getStringByField(Constraints.wordFileds);
            Integer type = tuple.getIntegerByField(Constraints.typeFileds);
            Key hk = new Key(key.getBytes());
		if (rel.equals("R")) {
           	  if(!bf_R.membershipTest(hk) && type.equals(1))
            	    	bf_R.add(hk);
            	  if(bf_R.membershipTest(hk) && type.equals(0))
                	bf_R.delete(hk);
           	}else{
           	  if(!bf_S.membershipTest(hk) && type.equals(1))
            	    	bf_S.add(hk);
            	  if(bf.membershipTest(hk) && type.equals(0))
                	bf_S.delete(hk);
		}
        }
        collector.ack(tuple);
    }

    public void cleanup(){
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constraints.coinFileds, new Fields(Constraints.relFileds,Constraints.wordFileds,Constraints.coinCountFileds));
        declarer.declareStream(Constraints.hotRFileds, new Fields(SCHEMA));
        declarer.declareStream(Constraints.nohotRFileds, new Fields(SCHEMA));
        declarer.declareStream(Constraints.hotSFileds, new Fields(SCHEMA));
        declarer.declareStream(Constraints.nohotSFileds, new Fields(SCHEMA));
    }

}
