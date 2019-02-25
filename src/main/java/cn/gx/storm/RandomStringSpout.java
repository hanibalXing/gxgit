package cn.gx.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.ThreadLocalRandom.current;


/**
 * @author gx
 * @ClassName: RandomStringSpout
 * @Description: java类作用描述
 * @date 2019/1/11 11:22
 * @Version: 1.0
 * @since
 */
public class RandomStringSpout extends BaseRichSpout {

	private final static Map<Integer, String> map=new HashMap<>(16);
	private SpoutOutputCollector collector;

	public RandomStringSpout(){
		map.put(0,"gx");
		map.put(1,"gxx");
		map.put(2,"gxxx");
		map.put(3,"gxxxx");
		map.put(4,"gxxxxx");

	}

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		System.out.println("===================open");
		this.collector=spoutOutputCollector;
	}

	@Override
	public void nextTuple() {
		collector.emit(new Values(map.get(current().nextInt(5))));
		try {
			TimeUnit.SECONDS.sleep(5);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("stream"));
	}
}
