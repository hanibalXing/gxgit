package cn.gx.storm.tick;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gx
 * @ClassName: NumberGenerateSpout
 * @Description: java类作用描述
 * @date 2019/2/10 18:04
 * @Version: 1.0
 * @since
 */
public class NumberGenerateSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    //private AtomicInteger integer;
    private static final Logger LOG= LoggerFactory.getLogger(NumberGenerateSpout.class);
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        LOG.info("spout open");
        this.collector=spoutOutputCollector;
        //this.integer=new AtomicInteger(1);
    }

    @Override
    public void nextTuple() {
        int value=Data.integer.getAndIncrement();
        collector.emit(new Values(value),value);
        LOG.info("spout:{}",value);
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void ack(Object msgId) {
        LOG.info("the value {}",msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("number"));
    }
}
