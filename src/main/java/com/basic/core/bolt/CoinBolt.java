package com.basic.core.bolt;

import com.basic.core.Constraints;
import com.basic.core.util.PredictHeavyLoadKeyUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;
import static com.basic.core.Constraints.Threshold_r;

/**
 * locate com.basic.storm.bolt
 * Created by windy on 2019/1/2
 */
public class CoinBolt extends BaseRichBolt {
    private OutputCollector collector;
    private PredictHeavyLoadKeyUtil predictHeavyLoadKeyUtil=PredictHeavyLoadKeyUtil.getPredictHeavyLoadKeyUtilInstance();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String rel  = tuple.getStringByField(Constraints.relFileds);
        String word = tuple.getStringByField(Constraints.wordFileds);
        Integer basecount = tuple.getIntegerByField(Constraints.coinCountFileds);
        int coincount = predictHeavyLoadKeyUtil.countCointUtilUp();
        if(coincount+basecount>=Threshold_r)
            collector.emit(new Values(rel,word,coincount));
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constraints.relFileds,Constraints.wordFileds,Constraints.coinCountFileds));
    }

}
