package cn.gx.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author gx
 * @ClassName: CallLogTrident
 * @Description: java类作用描述
 * @date 2019/2/16 5:13
 * @Version: 1.0
 * @since
 */
public class CallLogTrident {
    private static final Logger LOG= LoggerFactory.getLogger(CallLogTrident.class);
    public static void main(String[] args) throws InterruptedException {
        final FixedBatchSpout spout = new FixedBatchSpout(new Fields("caller","callee","time"), 10,
                new Values("1","3",5),
                new Values("1","3",5),
                new Values("1","3",5),
                new Values("2","4",8),
                new Values("4","7",9),
                new Values("6","9",11),
                new Values("5","1",1),
                new Values("1","8",55),
                new Values("8","7",23)
        );
        TridentTopology tredent = new TridentTopology();
        //按呼叫人group ,按通话时长count 类似group by(xx) sum(yy)
        tredent.newStream("telephon-time", spout)
                .shuffle()
                .groupBy(new Fields("caller"))
                .aggregate(new Fields("time"),new TimeSumReducer(),new Fields("sumtime"))
                .parallelismHint(5)
                //.newValuesStream()
                .peek(tridentTuple -> LOG.info("{}{}",tridentTuple.getFields(),tridentTuple));

        //按呼叫人和被呼叫人group,按个数count 类似group by(xx) count(yy)
        tredent.newStream("telephon-state", spout)
                .shuffle()
                .groupBy(new Fields("caller","callee"))
                .aggregate(new Count(),new Fields("state"))
                .parallelismHint(5)
                //.newValuesStream()
                .peek(tridentTuple -> LOG.info("{}{}",tridentTuple.getFields(),tridentTuple));

        final Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("CallLogTrident", config, tredent.build());
        TimeUnit.SECONDS.sleep(5);
        cluster.killTopology("CallLogTrident");
        cluster.shutdown();
    }

    private static class TimeSumReducer implements ReducerAggregator<Integer> {
        @Override
        public Integer init() {
            return 0;
        }

        @Override
        public Integer reduce(Integer integer, TridentTuple tridentTuple) {
            return integer+tridentTuple.getIntegerByField("time");
        }
    }
}
