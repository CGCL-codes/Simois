package com.basic.core.bolt;

import java.text.DecimalFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import static com.google.common.collect.Lists.newLinkedList;

//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.BasicOutputCollector;
//import backtype.storm.topology.OutputFieldsDeclarer;
//import backtype.storm.topology.base.BaseBasicBolt;
//import backtype.storm.tuple.Fields;
//import backtype.storm.tuple.Tuple;
//import backtype.storm.tuple.Values;
import com.basic.core.util.FileWriter;
import com.basic.core.util.Stopwatch;
import static com.basic.core.util.CastUtils.getBoolean;
import static com.basic.core.util.CastUtils.getInt;
import static com.basic.core.util.CastUtils.getList;
import static com.basic.core.util.CastUtils.getLong;
import static com.basic.core.util.CastUtils.getString;

public class JoinBolt extends BaseBasicBolt
{

    private static final List<String> SCHEMA = ImmutableList.of("relation",
            "timestamp", "key", "value");
    private static final long PROFILE_REPORT_PERIOD_IN_SEC = 1;
    private static final int BYTES_PER_TUPLE_R = 64;
    private static final int BYTES_PER_TUPLE_S = 56;
    private static final int BYTES_PER_MB = 1024 * 1024;

    private static final Logger LOG = getLogger(JoinBolt.class);

    private final String _rel;

    public JoinBolt(String relation) {
        super();
        _rel = relation;

        checkState(_rel.equals("R") || _rel.equals("S"), "Unknown relation: "
                + _rel);
    }

    private int _tid;

    private FileWriter _output;

    private int _thisJoinFieldIdx;
    private int _oppJoinFieldIdx;
    private String _operator;

    private boolean _window;
    private long _thisWinSize;
    private long _oppWinSize;

    private int _subindexSize;
    private Queue<Pair> _indexQueue;
    private Multimap<Object, Values> _currMap;

    private long _profileReportInSeconds;
    private long _triggerReportInSeconds;
    private Stopwatch _stopwatch;
    private DecimalFormat _df;

    private long _tuplesStored;
    private long _tuplesJoined;
    private int _thisTupleSize;
    private int _oppTupleSize;

    @Override
    public void prepare(Map conf, TopologyContext context) {
        _tid = context.getThisTaskId();

        String outputDir = getString(conf.get("outputDir"));
        String prefix = "srj_joiner_" + _rel.toLowerCase() + _tid;
//        _output = (new FileWriter(outputDir, prefix, "txt")).setFlushSize(10)
//                .setPrintStream(System.out);
        _output = new FileWriter("/tmp/", prefix, "txt");
        _subindexSize = getInt(conf.get("subindexSize"));

        LOG.info("relation:" + _rel + ", join_field_idx(this):"
                + _thisJoinFieldIdx + ", join_field_idx(opp):"
                + _oppJoinFieldIdx + ", operator:" + _operator + ", window:"
                + _window + ", win_size:" + _thisWinSize + ", subindex_size:"
                + _subindexSize);

        /* indexes */
        _indexQueue = newLinkedList();
        _currMap = LinkedListMultimap.create(_subindexSize);

        /* profiling */
        _tuplesStored = 0;
        _tuplesJoined = 0;

        _df = new DecimalFormat("0.00");
        _profileReportInSeconds = PROFILE_REPORT_PERIOD_IN_SEC;
        _triggerReportInSeconds = _profileReportInSeconds;
        _stopwatch = Stopwatch.createStarted();
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String rel = tuple.getStringByField("relation");
        if (!rel.equals("R") && !rel.equals("S")) {
            LOG.error("Unknown relation: " + rel);
            return;
        }

        if (rel.equals(_rel)) {
            store(tuple);
            ++_tuplesStored;
            collector.emit(new Values(tuple.getStringByField("relation")
                    , tuple.getLongByField("timestamp")
                    , tuple.getIntegerByField("key")
                    , tuple.getStringByField("value")));
        }
        else { // rel.equals(Opp(_rel))
            join(tuple, collector);
            ++_tuplesJoined;
        }

//        if (isTimeToOutputProfile()) {
//            output(getProfile());
//        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SCHEMA));
    }

    @Override
    public void cleanup() {
        _stopwatch.stop();

        StringBuilder sb = new StringBuilder();

        sb.append("relation:" + _rel);
        sb.append(", num_of_indexes:" + (_indexQueue.size() + 1));

        output(sb.toString());

        if (_output != null)
            _output.endOfFile();
    }

    private void store(Tuple tuple) {
        Long ts = tuple.getLongByField("timestamp");
        int key = tuple.getIntegerByField("key");
        String value = tuple.getStringByField("value");

        Values values = new Values(ts, key, value);

        _currMap.put(key, values);

        if (_currMap.size() >= _subindexSize) {
            _indexQueue.offer(ImmutablePair.of(ts, _currMap));
            _currMap = LinkedListMultimap.create(_subindexSize);
        }
    }

    private void join(Tuple tupleOpp, BasicOutputCollector collector) {
        /* join with archive indexes */
        int numToDelete = 0;
        long tsOpp = tupleOpp.getLongByField("timestamp");
        for (Pair pairTsIndex : _indexQueue) {
            long ts = getLong(pairTsIndex.getLeft());
            if (_window && !isInWindow(tsOpp, ts)) {
                ++numToDelete;
                continue;
            }

            join(tupleOpp, pairTsIndex.getRight(), collector);
        }

        for (int i = 0; i < numToDelete; ++i) {
            _indexQueue.poll();
        }

        /* join with current index */
        join(tupleOpp, _currMap, collector);
    }

    private void join(Tuple tupleOpp, Object index,
            BasicOutputCollector collector) {
        int key = tupleOpp.getIntegerByField("key");

        for (Values record : getMatchings(index, key)) {
            if (_rel.equals("R")) {
                output("R: " + value + " ---- " + tupleOpp.getStringByField("value"));
            }
            else { // _rel.equals("S")
                output("S: " + tupleOpp.getStringByField("value") + " ---- " + value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Collection<Values> getMatchings(Object index, Object value) {
        return ((Multimap<Object, Values>) index).get(value);
    }

    @SuppressWarnings("unchecked")
    private int getIndexSize(Object index) {
        return ((Multimap<Object, Values>) index).size();
    }

    @SuppressWarnings("unchecked")
    private int getNumTuplesInWindow() {
        int numTuples = 0;
        for (Pair pairTsIndex : _indexQueue) {
            numTuples += ((Multimap<Object, Values>) pairTsIndex.getRight())
                    .size();
        }
        numTuples += _currMap.size();

        return numTuples;
    }

    private boolean isInWindow(long tsIncoming, long tsStored) {
        long tsDiff = tsIncoming - tsStored;

        if (tsDiff >= 0) {
            return (tsDiff <= _thisWinSize);
        }
        else {
            return (-tsDiff <= _oppWinSize);
        }
    }

    private boolean isTimeToOutputProfile() {
        long currTime = _stopwatch.elapsed(SECONDS);

        if (currTime >= _triggerReportInSeconds) {
            _triggerReportInSeconds = currTime + _profileReportInSeconds;
            return true;
        }
        else {
            return false;
        }
    }

    private String getProfile() {
        StringBuilder sb = new StringBuilder();

        sb.append("[Joiner-" + _rel + "-" + _tid);
        sb.append(" @ " + _stopwatch.elapsed(SECONDS) + " sec]");

        sb.append(" #indexes=" + (_indexQueue.size() + 1));

        int tuplesInWindow = getNumTuplesInWindow();
        double sizeInWindow = ((double) (tuplesInWindow * _thisTupleSize))
                / BYTES_PER_MB;
        sb.append(", tuples(window)=" + tuplesInWindow + " ("
                + _df.format(sizeInWindow) + " MB)");

        sb.append(", tuples(stored)=" + _tuplesStored);

        double sizeJoined = ((double) (_tuplesStored * _oppTupleSize))
                / BYTES_PER_MB;
        sb.append(", tuples(joined)=" + _tuplesJoined + " ("
                + _df.format(sizeJoined) + " MB)");

        return sb.toString();
    }

    private void output(String msg) {
        if (_output != null)
            _output.write(msg);
    }
}

