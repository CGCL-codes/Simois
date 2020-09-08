# Simois

Simois is a scalable distributed stream join system, which supports efficient join operations in two streams with highly skewed data distribution. Simois can support the completeness of the join results, and greatly outperforms the existing stream join systems in terms of system throughput and the average processing latency. Experiment results show that Simois improves the system throughput significantly by 52% and reduces the average latency by 37%, compared to existing stream join systems.


## Introduction

Recently, more and more applications raise the demand of processing and analyzing the streaming data in real-time. Stream joining is one of the most basic and important operation to extract valuable information from multiple data streams. Processing joining on data streams is much more challenging than performing traditional join operation on databases. Firstly, stream joining needs to ensure the completeness of the join results, that means each pair of tuples from two streams should be matched exactly once, while the two streams are continuous and unbounded. Secondly, stream joining always needs to be processed in-memory to avoid frequently disk I/O. However, memory is always limited but the data are continuous. Thirdly, stream join applications usually need to report results in near real-time style, thus, performing efficient stream joining requires high throughput and low latency processing systems.

To support continuous stream joining, much effort has been done based on different parallel or distributed designs. Due to the limitation of memory capacity, a parallel stream join system commonly processes the stream join with a limited sliding window. Such designs actually significantly sacrifice the completeness of the results of stream join to achieve feasibility of the designs. Different from parallel systems, distributed join systems can support near full history join operations. 

Recently, Lin et al. propose a scalable distributed stream join system called BiStream (http://www.comp.nus.edu.sg/~linqian/bistream). BiStream depends on a novel stream joining model, which separates all the processing units for join operation into two symmetry sets and organizes them into a bipartite graph architecture. A key problem in BiStream is how to rationally partition tuples among different join units. BiStream introduces two partitioning strategies including random partitioning and hash partitioning. 

Random partitioning strategy partitions tuples of one stream to a random processing unit for storing, while the tuple of the other stream is broadcast to all the processing units for joining. Such a strategy in fact compares every two tuples in different streams regardless of the content of the tuples. Therefore, it incurs a large amount of unnecessary join operations, resulting in high latency and low throughput, especially for equi-joins. Hash partitioning strategy is a straightforward strategy for equi-joins. This strategy stores the tuples associated with a specific key in one stream to a specified processing unit using hashing. The tuples with the same key in the other stream will be hashed to the same unit for joining. However, such a strategy suffers from the load imbalance problem due to the skewed distribution of the keys, which is common in real world data. Thus, the processing units assigned the top heavy load become stragglers in the system and make the system inefficient.

To deal with the above problems, we build Simois, a {S}calable d{I}stribute strea{M} j{OI}n {S}ystem. Simois can efficiently identify the exact keys which incur heavy workload in real-time stream joining. With the identification, the system accordingly evenly partitions the keys causing heavy-load using shuffling strategy, and partitions the rest stream data using hash-based schemes to avoid redundant join computations.

## Structure of Simois

![image](https://github.com/DStream-Storm/Simois/blob/master/src/main/SimoisStructure.png)

Simois follows the join-biclique stream joining framework in BiStream. To implement the join-biclique model, we divide all the processing units into two sets: $R$-joining instances and $S$-joining instances. A $S$-joining instance stores the incoming tuples of stream $S$ based on the hash partitioning. The instance performs join operation whenever it receives a tuple of stream $R$, which has the associated key. Specifically, it matches the received $R$ tuple with all the $S$ tuples stored in this instance. $R$-joining instances are on the opposite. For simplicity, in the following, we only discuss about the $S$-joining instances and the symmetric $R$-instances work in the same way.

The key design in Simois is the light-weighted potential heavy-load keys predictor and the differentiated scheduling strategy for both streams. Simois consists of two main components: 1) an independent predicting component for identifying the keys which incur the heaviest workload, and 2) a scheduling component in each dispatching instance. The predicting component is a standalone component separated from the original processing procedure, which collects the information of all the tuples and uses this information to identify the keys with heaviest workload. The scheduling component is embedded in each processing element instance, which stores the identified heavy-load keys and supports fast querying and efficiently scheduling.


## How to use?

### Environment

We implement Simois atop Apache Storm (version 1.0.2 or higher), and deploy the system on a cluster. Each machine is equipped with an octa-core 2.4GHz Xeon CPU, 64.0GB RAM, and a 1000Mbps Ethernet interface card. One machine in the cluster serves as the master node to host the Storm Nimbus. The other machines run Storm supervisors.

### Initial Setting

Install Apache Storm (Please refer to http://storm.apache.org/ to learn more).

Install Apace Maven (Please refer to http://maven.apache.org/ to learn more).

Build and package the example

```txt
mvn clean package -Dmaven.test.skip=true
```

### Configurations

Configurations including Threshold_r, Threshold_l and Threshold_p in ./src/main/resources/simois.properties.

```txt
Threshold_r=12 (by default)
Threshold_l=24 (by default)
Threshold_p=0.03 (by default)
```

### Using Simois

Import SchedulingTopologyBuilder in the application source code

```txt
import com.basic.core.SchedulingTopologyBuilder;
```

Build SchedulingTopologyBuilder before the building of the topology

```txt
SchedulingTopologyBuilder builder=new SchedulingTopologyBuilder();
```

Generate SchedulingTopologyBuilder according to the Threshold_r, Threshold_l and Threshold_p (config in ./src/main/resources/dstream.properties).

```java
SchedulingTopologyBuilder builder=new SchedulingTopologyBuilder();
```

Set join-biclique and heavy-load key predictor in the application topology (see ./src/main/java/com/basic/core/KafkaTopology.java

```java
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
```

## Publications

If you want to know more detailed information, please refer to this paper:

Fan Zhang, Hanhua Chen, Hai Jin. "Simois: A Scalable Distributed Stream Join System with Skewed Workloads." in Proceedings of IEEE 39th International Conference on Distributed Computing Systems (ICDCS), 2019. ([Bibtex](https://github.com/CGCL-codes/Simois/blob/master/Simois-conf.bib))


## Authors and Copyright

Simois is developed in National Engineering Research Center for Big Data Technology and System, Cluster and Grid Computing Lab, Services Computing Technology and System Lab, School of Computer Science and Technology, Huazhong University of Science and Technology, Wuhan, China by Fan Zhang(zhangf@hust.edu.cn), Hanhua Chen (chen@hust.edu.cn), Hai Jin (hjin@hust.edu.cn).

Copyright (C) 2019, [STCS & CGCL](http://grid.hust.edu.cn/) and [Huazhong University of Science and Technology](http://www.hust.edu.cn).


