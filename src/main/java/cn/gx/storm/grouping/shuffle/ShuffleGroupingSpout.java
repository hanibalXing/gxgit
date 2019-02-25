package cn.gx.storm.grouping.shuffle;

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

import static java.lang.Thread.currentThread;

/**
 * @author gx
 * @ClassName: StringGeneratorSpout
 * @Description: java类作用描述
 * @date 2019/1/14 21:38
 * @Version: 1.0
 * @since
 */
public class ShuffleGroupingSpout extends BaseRichSpout {

	private final static Logger logger = LoggerFactory.getLogger(ShuffleGroupingSpout.class);
	private TopologyContext context;
	private SpoutOutputCollector collector;
	private AtomicInteger ai;

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
	this.context=topologyContext;
	this.collector=spoutOutputCollector;
	this.ai=new AtomicInteger();
	logger.warn("StringGeneratorSpout->open:hashCode:{}->Thread:{}->taskId{}",
			this.hashCode(),currentThread().getName(),context.getThisTaskId());
	}

	@Override
	public void nextTuple() {
		int i=ai.incrementAndGet();
		if (i<10) {
			logger.warn("StringGeneratorSpout->open:hashCode:{}->Thread:{}->taskId{}:values:{}"
					,this.hashCode(),currentThread().getName(),context.getThisTaskId(),i);
			collector.emit(new Values(i));
		}
		try {
			TimeUnit.SECONDS.sleep(5);
		} catch (InterruptedException e) {

		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("i"));
	}

	@Override
	public void close() {
		logger.warn("StringGeneratorSpout->close:hashCode:{}->Thread:{}->taskId{}"
				,this.hashCode(),currentThread().getName(),context.getThisTaskId());
		super.close();
	}
}
