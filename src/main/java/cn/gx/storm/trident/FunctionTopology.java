package cn.gx.storm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author gx
 * @ClassName: FunctionTopology
 * @Description: java类作用描述
 * @date 2019/2/13 4:19
 * @Version: 1.0
 * @since
 */
public class FunctionTopology {
    private static final Logger LOG= LoggerFactory.getLogger(FunctionTopology.class);
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {
        boolean isRemote=false;
        if (args.length>0) {
            isRemote=true;
        }
        FixedBatchSpout spout=new FixedBatchSpout(new Fields("word"),3,
                new Values("gx 321 123 3fda21"),
                new Values("123 xg 123 456eaf7"),
                new Values("123 777 3xczv21 321"),
                new Values("xxx 888 fgdg yyy")
                );
        spout.setCycle(false);
        TridentTopology topology=new TridentTopology();
        final Config config=new Config();
        config.setNumWorkers(2);
        config.setDebug(false);

        topology.newStream("hellotd",spout).parallelismHint(1)
                .partition(Grouping.local_or_shuffle(new NullStruct()))
                //过滤数据
                .each(new Fields("word"),new MyStrFilter())
                .parallelismHint(2)

                .partition(Grouping.local_or_shuffle(new NullStruct()))
                //变大写,把输出的fields改为upperResult
                .map(new MyMapFunction(), new Fields("upperResult"))
                .parallelismHint(2)

                .partition(Grouping.local_or_shuffle(new NullStruct()))
                //去空格,产生一个新的filds叫trimResult
                .each(new Fields("upperResult"),new MyTrimFunciton(),new Fields("trimResult"))
                .parallelismHint(2)

                .partition(Grouping.fields(ImmutableList.of("upperResult","trimResult")))
                //打印结果
                .peek(input->LOG.info("the upper result is {} the trim result is {} "
                        ,input.getStringByField("upperResult"),input.getStringByField("trimResult")))
                .parallelismHint(2)

                //去除upperResult
                .partition(Grouping.local_or_shuffle(new NullStruct()))
                //只要留下的fields
                .project(new Fields("trimResult"))
                .parallelismHint(2)

                .partition(Grouping.local_or_shuffle(new NullStruct()))
                //打印结果
                .peek(input->LOG.info("the  result is {}",input))
                .parallelismHint(2);

        if (isRemote) {
            StormSubmitter.submitTopology("FunctionTopology",config,topology.build());
        } else {
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology("FunctionTopology",config,topology.build());
            TimeUnit.SECONDS.sleep(10);
            cluster.killTopology("FunctionTopology");
            cluster.shutdown();
        }
    }

    private static class MyStrFilter extends BaseFilter {

        @Override
        public boolean isKeep(TridentTuple tridentTuple) {
            return !tridentTuple.getStringByField("word").contains("xczv");
        }
    }

    private static class MyMapFunction implements MapFunction {
        private static final Logger LOG= LoggerFactory.getLogger(MyMapFunction.class);
        @Override
        public Values execute(TridentTuple tridentTuple) {
            return new Values(tridentTuple.getStringByField("word").toUpperCase());
        }
    }

    private static class MyTrimFunciton extends BaseFunction {

        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            tridentCollector.emit(new Values(tridentTuple.getStringByField("upperResult")
                    .replaceAll(" ", "")));
        }
    }



}
