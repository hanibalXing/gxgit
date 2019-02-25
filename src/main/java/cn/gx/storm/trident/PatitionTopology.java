package cn.gx.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author gx
 * @ClassName: PatitionTopology
 * @Description: java类作用描述
 * @date 2019/2/14 1:55
 * @Version: 1.0
 * @since
 */
public class PatitionTopology {
    private static final Logger LOG= LoggerFactory.getLogger(PatitionTopology.class);
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        boolean isRemote = false;
        if (args.length > 0) {
            isRemote = true;
        }
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("word"), 3,
                new Values("1"),
                new Values("2"),
                new Values("3"),
                new Values("4"),
                new Values("5"),
                new Values("6")
        );
        spout.setCycle(false);
        TridentTopology topology = new TridentTopology();
        final Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(false);

        topology.newStream("hellotd", spout).parallelismHint(1)

                //相同的fileds去往同一个excutor
                .partition(Grouping.fields(ImmutableList.of("word")))
                .peek(input -> LOG.info("the filed result is {} " , input.getStringByField("word")))
                .parallelismHint(2)

                //去往一个excutor
                .global()
                .peek(input -> LOG.info("the global result is {}", input))
                .parallelismHint(3)

                //同一个batch的数据去到相同的excutor
                .batchGlobal()
                .peek(input -> LOG.info("the batchGlobal result is {}", input))
                .parallelismHint(2)

                .partition(Grouping.local_or_shuffle(new NullStruct()))
                .peek(input -> LOG.info("the shuffle result is {}", input))
                .parallelismHint(3)

                //去往所有excutor且每个excutor都得到的完整的数据
                .broadcast()
                .peek(input -> LOG.info("the broadcast result is {}", input))
                .parallelismHint(2)

                //去往自定义的taskId最大的excutor
                .partition(new MaxTaskIdGrouping())
                .peek(input -> LOG.info("the custom result is {}", input))
                .parallelismHint(2)

                ;
        if (isRemote) {
            StormSubmitter.submitTopology("FunctionTopology", config, topology.build());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("PatitionTopology", config, topology.build());
            TimeUnit.SECONDS.sleep(15);
            cluster.killTopology("PatitionTopology");
            cluster.shutdown();
        }
    }
}
