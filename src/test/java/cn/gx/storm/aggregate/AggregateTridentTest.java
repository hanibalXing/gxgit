package cn.gx.storm.aggregate;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author gx
 * @ClassName: AggregateTridentTest
 * @Description: java类作用描述
 * @date 2019/2/15 1:15
 * @Version: 1.0
 * @since
 */
public class AggregateTridentTest {
    private static final Logger LOG= LoggerFactory.getLogger(AggregateTridentTest.class);
    private FixedBatchSpout spout;
    private FixedBatchSpout spout1;
    @Before
    public void setUp(){
        Values[] values=new Values[10];
        for(int i=0;i<values.length;i++) {
            values[i]= new Values(String.valueOf(current().nextInt(5)),current().nextInt(100));
        }
        spout=new FixedBatchSpout(new Fields("name","age"),3,values);
               /* new Values("gx",33),
                new Values("gzh",26),
                new Values("dfasfdsafdsafsda",29),
                new Values("gdj",63),
                new Values("yfdsxj",61),
                new Values("yxfdsaj",68),
                new Values("fdasyxj",79),
                new Values("yfdsxj",61),
                new Values("yxfdsaj",68),
                new Values("fasyxj",59),
                new Values("yfdtxj",61),
                new Values("gfdsaj",33),
                new Values("qqasyxj",22)
        );*/
        spout1=new FixedBatchSpout(new Fields("name","age"),3,
                new Values("gx",33),
                new Values("gx",26),
                new Values("wj",29),
                new Values("gdj",63),
                new Values("gdj",61),
                new Values("1",68),
                new Values("2",79),
                new Values("3",61),
                new Values("4",68),
                new Values("5",59),
                new Values("4",61),
                new Values("4",33)
        );
        spout.setCycle(false);
        spout1.setCycle(false);
    }
    @Test
    public void testPartitionAggregate() throws InterruptedException {
        final TridentTopology tridentTopology=new TridentTopology();
        tridentTopology.newStream("testpartition",spout)
                .shuffle()
                //就是把每个batch里面的数据聚合成一个，那么采用count聚合后生成的就是每个batch里面的values个数
                .partitionAggregate(new Fields("name","age"),new Count(),new Fields("count"))
                .parallelismHint(3)
                .each(new Fields("count"),new LogFilter("count",Long.class));
        this.submitTopoloyThenKill("testpartition",tridentTopology.build(),10);

    }

    @Test
    public void testAggregate() throws InterruptedException {
        final TridentTopology tridentTopology=new TridentTopology();
        tridentTopology.newStream("testpartition",spout)
                //每个excutor得到的不一定是完整的batch数据
                .partitionBy(new Fields("age"))
                .each(new Fields("name"),new LogFilter("name",String.class))
                .parallelismHint(2)
                //每个excutor得到的都是完整的batch数据，即3 3 1，这3个count结果会被两个线程执行
                .aggregate(new Fields("name","age"),new Count(),new Fields("count"))
                .parallelismHint(2)
                .each(new Fields("count"),new LogFilter("count",Long.class));
        this.submitTopoloyThenKill("testpartition",tridentTopology.build(),5);

    }

    @Test
    public void testReduce() throws InterruptedException {
        final TridentTopology tridentTopology=new TridentTopology();
        tridentTopology.newStream("testpartition",spout)
                //每个excutor得到的都是完整的batch数据，即3 3 1，这3个年龄和结果会被两个线程执行
                .aggregate(new Fields("name","age"),new AgeSumReducer(),new Fields("agesum"))
                .parallelismHint(5)
                .each(new Fields("agesum"),new LogFilter("agesum",Integer.class));

        this.submitTopoloyThenKill("testpartition",tridentTopology.build(),5);

    }

    @Test
    public void testCombiner() throws InterruptedException {
        final TridentTopology tridentTopology=new TridentTopology();
        tridentTopology.newStream("testCombiner",spout)
                //CombinerAggregator首先在每个分区上运行partitionAggregate，在每个partition内先聚合，
                // 然后运行全局重新分区(global)操作以合并同一批次的所有分区到一个单独的分区，
                // 即把前面每个partition聚合的结果，再放到一个单独的partition进行聚合。
                // 这里的网络传输与其他两个聚合器相比较少。 因此，CombinerAggregator的总体性能比Aggregator和ReduceAggregator好。
                .partitionBy(new Fields("name"))
                .aggregate(new Fields("age"),new AgeSumCombiner(),new Fields("agesum"))
                .parallelismHint(5)
                .each(new Fields("agesum"),new LogFilter("agesum",Integer.class));

        this.submitTopoloyThenKill("testCombiner",tridentTopology.build(),5);

    }

    @Test
    public void testPersistence() throws InterruptedException {
        final TridentTopology tridentTopology=new TridentTopology();
        tridentTopology.newStream("testPersistence",spout)

                .partitionBy(new Fields("name"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("age"),new AgeSumCombiner(), new Fields("sum"))
                .parallelismHint(2)
                .newValuesStream()
                .shuffle()
                .each(new Fields("sum"),new LogFilter("sum",Integer.class))
                .parallelismHint(2)
        ;

        this.submitTopoloyThenKill("testPersistence",tridentTopology.build(),5);

    }

    @Test
    public void testChainAggTrident() throws InterruptedException {
        final TridentTopology tridentTopology=new TridentTopology();

        tridentTopology.newStream("testChainAggTrident",spout)
                .partitionBy(new Fields("name"))
                .chainedAgg()
                //把数据广播到3个地方执行后放到以个tuple中
                .aggregate(new Fields("name"),new Count(),new Fields("count1"))
                .aggregate(new Fields("name"),new Count(),new Fields("count2"))
                .aggregate(new Fields("age"),new Sum(),new Fields("sum"))
                .chainEnd()
                .partitionBy(new Fields("sum"))
                .peek(tridentTuple -> LOG.info("{}{}",tridentTuple.getFields(),tridentTuple))
                .parallelismHint(2)
        ;

        this.submitTopoloyThenKill("testChainAggTrident",tridentTopology.build(),5);

    }
    @Test
    public void testGroupBy() throws InterruptedException {
        final TridentTopology tridentTopology=new TridentTopology();
        LOG.info("{}",spout.toString());
        tridentTopology.newStream("testGroupBy",spout1)
                .partitionBy(new Fields("name"))
                //按batch group by
                .groupBy(new Fields("name"))
                .aggregate(new Count(),new Fields("count"))
                .peek(tridentTuple -> LOG.info("{}{}",tridentTuple.getFields(),tridentTuple))
                .parallelismHint(2)
        ;

        this.submitTopoloyThenKill("testGroupBy",tridentTopology.build(),5);

    }

    private void submitTopoloyThenKill(String name, StormTopology topology,int secondsTime) throws InterruptedException {
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(name, getConf(), topology);
        TimeUnit.SECONDS.sleep(secondsTime);
        cluster.killTopology(name);
        cluster.shutdown();
    }

    private Config getConf()
    {
     final Config conf=new Config();
     conf.setNumWorkers(3);
     conf.setDebug(false);
     return conf;
    }

    private static class LogFilter extends BaseFilter {
        private static final Logger LOG= LoggerFactory.getLogger(LogFilter.class);
        private String filedName;
        private Class type;
        public LogFilter(String filedNmae,Class type){
            this.filedName=filedNmae;
            this.type=type;
        }
        @Override
        public boolean isKeep(TridentTuple tridentTuple) {

            if (type==Long.class) {
                LOG.info("result is {} ", tridentTuple.getLongByField(filedName));
            }
            if (type==Integer.class) {
                LOG.info("result is {} ", tridentTuple.getIntegerByField(filedName));
            }
            return true;
        }
    }

    private static class AgeSumReducer implements ReducerAggregator<Integer> {
        private static final Logger LOG= LoggerFactory.getLogger(AgeSumReducer.class);
        @Override
        public Integer init() {
            return 0;
        }

        @Override
        public Integer reduce(Integer integer, TridentTuple tridentTuple) {
            LOG.info("reduce valuse1 {} values2 {}",integer,tridentTuple.getIntegerByField("age"));
            return integer+tridentTuple.getIntegerByField("age");
        }
    }

    private static class AgeSumCombiner implements CombinerAggregator<Integer> {
        private static final Logger LOG= LoggerFactory.getLogger(AgeSumCombiner.class);
        @Override
        public Integer init(TridentTuple tridentTuple) {
            return tridentTuple.getInteger(0);
        }

        @Override
        public Integer combine(Integer integer, Integer t1) {
            LOG.info("combiner valuse1 {} values2 {}",integer,t1);
            return integer+t1;
        }

        @Override
        public Integer zero() {
            return 0;
        }
    }



}
