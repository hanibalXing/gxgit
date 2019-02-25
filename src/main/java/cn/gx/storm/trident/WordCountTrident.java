package cn.gx.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author gx
 * @ClassName: WordCountTrident
 * @Description: java类作用描述
 * @date 2019/2/16 4:36
 * @Version: 1.0
 * @since
 */
public class WordCountTrident {

    private static final Logger LOG= LoggerFactory.getLogger(WordCountTrident.class);

    public static void main(String[] args) throws InterruptedException {
        final FixedBatchSpout spout=new FixedBatchSpout(new Fields("line"),10,
                new Values("fdsa adsa v1x xxx"),
                new Values("fdsa fdsa v3x xxx"),
                new Values("fds1 fds2 vx2 xx4"),
                new Values("fds1 fdsa vx1 xx1"),
                new Values("fds1 fds4 vx2 xx4")
                );
        TridentTopology tredent = new TridentTopology();
        tredent.newStream("wordcount",spout)
                .shuffle()
                //把每个values split后发送出去
                .each(new Fields("line"),new Split(),new Fields("word"))
                .parallelismHint(2)
                .peek(tridentTuple -> LOG.info("{}",tridentTuple))
                //group by count
                .groupBy(new Fields("word"))
                //.persistentAggregate(new MemoryMapState.Factory(),new Count(),new Fields("count"))
                .aggregate(new Count(),new Fields("count"))
                .parallelismHint(5)
                //.newValuesStream()
                .peek(tridentTuple -> LOG.info("{}",tridentTuple));
        final Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("PatitionTopology", config, tredent.build());
        TimeUnit.SECONDS.sleep(5);
        cluster.killTopology("PatitionTopology");
        cluster.shutdown();
    }
}
