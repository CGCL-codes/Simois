package com.basic.core.bolt;

import java.util.Map;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

import static com.google.common.collect.Maps.newHashMap;

//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.base.BaseRichBolt;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
import com.basic.core.util.FileWriter;
import com.basic.core.util.Stopwatch;
import static com.basic.core.util.CastUtils.getBoolean;
import static com.basic.core.util.CastUtils.getLong;
import static com.basic.core.util.CastUtils.getString;

public class AggregateBolt extends BaseRichBolt
{
    private static final Logger LOG = getLogger(AggregateBolt.class);

    private FileWriter _output;

    private boolean _aggregate;

    private long _count;
    private long _sum;
    private double _avg;
    private long _min;
    private long _max;
    private Map<Integer, Values> _ledger;

    private long _aggReportInSeconds;
    private long _triggerEmitInSeconds;
    private Stopwatch _stopwatch;

    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        String outputDir = getString(conf.get("outputDir"));
        String prefix = "a" + context.getThisTaskId();
        _output = (new FileWriter(outputDir, prefix, "out")).setFlushSize(10)
                .setPrintStream(System.out);

        _aggregate = getBoolean(conf.get("aggregate"));
        _count = 0;
        if (_aggregate) {
            _sum = 0;
            _avg = 0.0;
            _min = Long.MAX_VALUE;
            _max = Long.MIN_VALUE;
        }
        _ledger = newHashMap();

        _aggReportInSeconds = getLong(conf.get("aggReportInSeconds"));
        _triggerEmitInSeconds = _aggReportInSeconds;

        LOG.info("aggregate:" + _aggregate + ", report_interval(sec):"
                + _aggReportInSeconds);

        _stopwatch = Stopwatch.createStarted();
    }

    public void execute(Tuple tuple) {
        aggregate(tuple);

        if (isTimeToOutputAggregate()) {
            output(getAggregate());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }

    public void cleanup() {
        _stopwatch.stop();

        output(getAggregate());

        if (_output != null)
            _output.endOfFile();
    }

    private void aggregate(Tuple tuple) {
        /* retrieve the ledger record */
        Integer srcId = Integer.valueOf(tuple.getSourceTask());
        Values values = _ledger.get(srcId);
        if (values == null) {
            values = new Values(Long.valueOf(0), Long.valueOf(0));
        }

        /* update count */
        long countOld = getLong(values, 0);
        long countNew = tuple.getLongByField("count");
        _count += countNew - countOld;
        values.set(0, Long.valueOf(countNew));

        /* update sum, avg, min and max */
        if (_aggregate) {
            long sumOld = getLong(values, 1);
            long sumNew = tuple.getLongByField("sum");
            _sum += sumNew - sumOld;
            values.set(1, Long.valueOf(sumNew));

            _avg = ((double) _sum) / _count;

            _min = Math.min(_min, tuple.getLongByField("min"));
            _max = Math.max(_max, tuple.getLongByField("max"));
        }

        /* update the ledger record */
        _ledger.put(srcId, values);
    }

    private boolean isTimeToOutputAggregate() {
        long currTime = _stopwatch.elapsed(SECONDS);

        if (currTime >= _triggerEmitInSeconds) {
            _triggerEmitInSeconds = currTime + _aggReportInSeconds;
            return true;
        }
        else {
            return false;
        }
    }

    private String getAggregate() {
        StringBuilder sb = new StringBuilder();

        sb.append("[GLOBAL AGG @ " + _stopwatch.elapsed(SECONDS) + " sec]");
        sb.append(" count=" + _count);
        if (_aggregate) {
            sb.append(", sum=" + _sum);
            sb.append(", avg=" + _avg);
            sb.append(", min=" + _min);
            sb.append(", max=" + _max);
        }

        return sb.toString();
    }

    private void output(String msg) {
        if (_output != null)
            _output.write(msg);
    }
}