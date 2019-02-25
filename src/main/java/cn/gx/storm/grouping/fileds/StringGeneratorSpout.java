package cn.gx.storm.grouping.fileds;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
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
public class StringGeneratorSpout extends BaseRichSpout {

	private final static Logger logger = LoggerFactory.getLogger(StringGeneratorSpout.class);
	private TopologyContext context;
	private SpoutOutputCollector collector;
	private List<String> strList;
	private int index=0;

	@Override
	public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
	this.context=topologyContext;
	this.collector=spoutOutputCollector;
	strList=Arrays.asList("xxg","gxx","xxg","gxx","xxg","gxx","xxg","gxx","xxg","gxx","xxg","gxx","xxg","gxx","xxg","gxx",
			"xxg","gxx","xxg","gxx","xxg","gxx","xxg","gxx","xxg","gxx","xxg","gxx","xxg","gxx","xxg","gxx");
	logger.warn("StringGeneratorSpout->open:hashCode:{}->Thread:{}->taskId{}",
			this.hashCode(),currentThread().getName(),context.getThisTaskId());
	}

	@Override
	public void nextTuple() {
		if (index<strList.size()) {
			String name=strList.get(index++);
			/*logger.warn("StringGeneratorSpout->open:hashCode:{}->Thread:{}->taskId{}:values:{}"
					,this.hashCode(),currentThread().getName(),context.getThisTaskId(),name);*/
			collector.emit(new Values(name));
		}
		try {
			TimeUnit.SECONDS.sleep(5);
		} catch (InterruptedException e) {

		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("name"));
	}

	@Override
	public void close() {
		logger.warn("StringGeneratorSpout->close:hashCode:{}->Thread:{}->taskId{}"
				,this.hashCode(),currentThread().getName(),context.getThisTaskId());
		super.close();
	}
}
