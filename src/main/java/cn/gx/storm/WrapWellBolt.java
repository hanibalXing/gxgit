package cn.gx.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @author gx
 * @ClassName: WrapWellBolt
 * @Description: java类作用描述
 * @date 2019/1/11 11:36
 * @Version: 1.0
 * @since
 */
public class WrapWellBolt extends BaseBasicBolt {
	@Override
	public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
		final String value = tuple.getStringByField("stream");
		System.out.println(value+"####");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}
}
