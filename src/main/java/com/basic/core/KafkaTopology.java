package com.basic.core;

import com.basic.core.Constraints;
import com.basic.core.SchedulingTopologyBuilder;
import com.basic.core.util.MyScheme;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import com.esotericsoftware.minlog.Log;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;
import static org.slf4j.LoggerFactory.getLogger;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import com.basic.core.bolt.*;
import com.basic.core.core.ContRandGrouping;
import com.basic.core.core.FastJoinGrouping;
import com.basic.core.util.FileWriter;

import java.util.Arrays;
import java.util.HashMap;

import static com.basic.core.util.LogHelpers.logTopology;
import static com.basic.core.util.StormRunner.runInCluster;
import static com.basic.core.util.StormRunner.runLocally;

import static com.basic.core.Constraints.SCHEDULER_BOLT_ID;

public class KafkaTopology
{
    private static final Logger LOG = getLogger(KafkaTopology.class);
    private static final String SHUFFLE_BOLT_ID = "shuffler";
    private static final String RESHUFFLE_BOLT_ID = "reshuffler";
    private static final String JOINER_R_BOLT_ID = "joiner-r";
    private static final String JOINER_S_BOLT_ID = "joiner-s";
    private static final String POST_PROCESS_BOLT_ID = "gatherer";
    private static final String AGGREGATE_BOLT_ID = "aggregator";

    public static final String KAFKA_SPOUT_ID_R ="kafka-spout-r";
    public static final String KAFKA_SPOUT_ID_S ="kafka-spout-s";
    //public static final String KAFKA_TEST_BOLT_ID = "kafka-test";
//    public static final String KAFKA_BROKER = "node24:9092,node25:9092,node26:9092,node27:9092,node28:9092";
    public static final String KAFKA_BROKER = "node95:9092,node96:9092,node97:9092,node98:9092,node99:9092";



    private final TopologyArgs _args = new TopologyArgs("KafkaTopology");

    public int run(String[] args) throws Exception {
        if (!_args.processArgs(args))
            return -1;
        if (_args.help)
            return 0;
        else
            // _args.logArgs();
            writeSettingsToFile();

        /* build topology */
        StormTopology topology = createTopology();
        if (topology == null)
            return -2;
        logTopology(LOG, topology);

        /* configure topology */
        Config conf = configureTopology();
        if (conf == null)
            return -3;
        LOG.info("configuration: " + conf.toString());
        LOG.info("groupid: " + _args.groupid);
        LOG.info("topic: " + _args.topic);

        /* run topology */
        if (_args.remoteMode) {
            LOG.info("execution mode: remote");
            runInCluster(_args.topologyName, topology, conf);
        }
        else {
            LOG.info("execution mode: local");
            writeSettingsToFile();
            runLocally(_args.topologyName, topology, conf, _args.localRuntime);
        }

        return 0;
    }

    private StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        SchedulingTopologyBuilder builder=new SchedulingTopologyBuilder();
        JoinBolt joinerR = new JoinBolt("R");
        JoinBolt joinerS = new JoinBolt("S");

        builder.setSpout(KAFKA_SPOUT_ID_R, new KafkaSpout<>(getKafkaSpoutConfig(KAFKA_BROKER, "didiOrder" + _args.dataSize, _args.groupid)), _args.numKafkaSpouts);
        builder.setSpout(KAFKA_SPOUT_ID_S, new KafkaSpout<>(getKafkaSpoutConfig(KAFKA_BROKER, "didiGps" + _args.dataSize, _args.groupid)), _args.numKafkaSpouts);
        builder.setBolt(SHUFFLE_BOLT_ID, new ShuffleBolt(_args.dataSize), _args.numShufflers)
                .shuffleGrouping(KAFKA_SPOUT_ID_R)
                .shuffleGrouping(KAFKA_SPOUT_ID_S);

	builder.setDifferentiatedScheduling(SHUFFLE_BOLT_ID, Constraints.relFileds, Constraints.wordFileds);

      builder.setBolt(JOINER_R_BOLT_ID, joinerR, _args.numPartitionsR)
                    .fieldsGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.nohotRFileds, new Fields(Constraints.wordFileds))
                    .fieldsGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.nohotSFileds, new Fields(Constraints.wordFileds))
                    .shuffleGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.hotRFileds)
                    .allGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.hotSFileds);

       builder.setBolt(JOINER_S_BOLT_ID, joinerS, _args.numPartitionsS)
                    .fieldsGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.nohotSFileds, new Fields(Constraints.wordFileds))
                    .fieldsGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.nohotRFileds, new Fields(Constraints.wordFileds))
                    .shuffleGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.hotSFileds)
                    .allGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.hotRFileds);


        builder.setBolt(POST_PROCESS_BOLT_ID, new PostProcessBolt(_args.numPartitionsR + _args.numPartitionsS), 1)
            .globalGrouping(JOINER_R_BOLT_ID, JOINER_TO_POST_STREAM_ID)
            .globalGrouping(JOINER_S_BOLT_ID, JOINER_TO_POST_STREAM_ID);
        return builder.createTopology();
    }

    private Config configureTopology() {
        Config conf = new Config();
        _args.topologyName += "-" + _args.strategy;
        conf.setDebug(_args.debug);
        conf.setNumWorkers(_args.numWorkers);
        conf.setNumAckers(_args.numShufflers);
        conf.put("joinFieldIdxR", _args.joinFieldIdxR);
        conf.put("joinFieldIdxS", _args.joinFieldIdxS);
        conf.put("operator", _args.operator);

        conf.put("fluctuation", _args.fluctuation);

        conf.put("subindexSize", _args.subindexSize);

        conf.put("window", _args.window);
        conf.put("winR", _args.winR);
        conf.put("winS", _args.winS);

        conf.put("dedup", !_args.noDedup);
        conf.put("dedupSize", _args.dedupSize);

        conf.put("aggregate", _args.aggregate);
        conf.put("aggReportInSeconds", _args.aggReportInSeconds);

        conf.put("noOutput", _args.noOutput);
        conf.put("outputDir", _args.outputDir);
        conf.put("simple", _args.simple);

        conf.put("intLower", _args.intLower);
        conf.put("intUpper", _args.intUpper);
        conf.put("doubleLower", _args.doubleLower);
        conf.put("doubleUpper", _args.doubleUpper);
        conf.put("charsLength", _args.charsLength);

        return conf;
    }

    private void writeSettingsToFile() {
        FileWriter output = new FileWriter(_args.outputDir, "top", "txt")
                .setPrintStream(System.out);
        _args.logArgs(output);
        output.endOfFile();
    }

    public static void main(String[] args) throws Exception {
        int rc = (new KafkaTopology()).run(args);
        LOG.info("return code: " + rc);
    }


    public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers,String topic, String groupid) {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
                new Fields("topic", "partition", "offset", "key", "value"));
        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{topic})
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, groupid)
                .setRetry(getRetryService())
                .setRecordTranslator(trans)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
                .build();
    }
    public static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(0),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

}