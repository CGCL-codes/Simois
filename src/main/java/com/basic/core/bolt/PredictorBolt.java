package com.basic.core.bolt;

import com.basic.core.Constraints;
import com.basic.core.inter.DumpRemoveHandler;
import com.basic.core.util.PredictHeavyLoadKeyUtil;
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
public class PredictorBolt extends BaseRichBolt {
    private OutputCollector collector;
    private PredictHeavyLoadKeyUtil predict_R=PredictHeavyLoadKeyUtil.getPredictHeavyLoadKeyUtilInstance();
    private PredictHeavyLoadKeyUtil predict_S=PredictHeavyLoadKeyUtil.getPredictHeavyLoadKeyUtilInstance();
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
       String rel = tuple.getStringByField(Constraints.relFileds);
        final String word = tuple.getStringByField(Constraints.wordFileds);
        Integer count = tuple.getIntegerByField(Constraints.coinCountFileds);

	  if (rel.equals("R")) {
       	 predict_R.PredictHeavyLoadKey(word,count);

       	 if(predict_R.isHeavyLoadKey(word))
         	   collector.emit(new Values(rel,word,1));
		predict_R.SynopsisHashMapRandomDump(new DumpRemoveHandler() {
            @Override
            public void dumpRemove(String key) {
                collector.emit(new Values(rel,key,0));
            }
        });
	  }else{
	       predict_S.PredictHeavyLoadKey(word,count);

       	 if(predict_S.isHeavyLoadKey(word))
         	   collector.emit(new Values(rel,word,1));
		predict_S.SynopsisHashMapRandomDump(new DumpRemoveHandler() {
            @Override
            public void dumpRemove(String key) {
                collector.emit(new Values(rel,key,0));
            }
        });
	  }

        collector.ack(tuple);
    }

    public void cleanup(){
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constraints.relFileds,Constraints.wordFileds,Constraints.typeFileds));
    }
}
