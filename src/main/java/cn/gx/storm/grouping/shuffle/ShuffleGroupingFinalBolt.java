package cn.gx.storm.grouping.shuffle;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static java.lang.Thread.currentThread;

/**
 * @author gx
 * @ClassName: ShuffleGroupingBolt
 * @Description: java类作用描述
 * @date 2019/1/14 21:48
 * @Version: 1.0
 * @since
 */
public class ShuffleGroupingFinalBolt extends BaseBasicBolt {

	private final static Logger logger = LoggerFactory.getLogger(ShuffleGroupingSpout.class);
	private TopologyContext context;
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.context=context;
		logger.warn("FiledGroupingFinalBolt->prepare:hashCode:{}->Thread:{}->taskId{}",
				this.hashCode(),currentThread().getName(),context.getThisTaskId());
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Integer i=tuple.getIntegerByField("result");

		logger.warn("FiledGroupingFinalBolt->execute:hashCode:{}->Thread:{}->taskId{},value{}",
				this.hashCode(),currentThread().getName(),context.getThisTaskId(),i);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("result"));
	}

	@Override
	public void cleanup() {
		logger.warn("FiledGroupingFinalBolt->cleanup:hashCode:{}->Thread:{}->taskId{},value{}",
				this.hashCode(),currentThread().getName(),context.getThisTaskId());
	}
}
