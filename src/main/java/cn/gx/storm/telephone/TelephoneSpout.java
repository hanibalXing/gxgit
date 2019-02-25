package cn.gx.storm.telephone;

import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.sql.Time;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author gx
 * @ClassName: TelephoneSpout
 * @Description: java类作用描述
 * @date 2019/2/11 5:05
 * @Version: 1.0
 * @since
 */
public class TelephoneSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<String> telephoneList;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
        telephoneList= ImmutableList.of(
                "13883092602",
                "13883092603",
                "13883092604",
                "13883092605",
                "13883092606",
                "13883092607",
                "13883092608",
                "13883092609"
                );
    }

    @Override
    public void nextTuple() {
        int size=telephoneList.size();
        String caller=telephoneList.get(current().nextInt(size));
        String callee=telephoneList.get(current().nextInt(size));
        while (caller.equals(callee)) {
            callee=telephoneList.get(current().nextInt(size));
        }
        double callTime=current().nextDouble(10);
        collector.emit(new Values(caller,callee,callTime));
        try {
            TimeUnit.SECONDS.sleep(current().nextInt(2));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("caller","callee","callTime"));
    }
}
