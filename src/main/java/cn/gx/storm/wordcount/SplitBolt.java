package cn.gx.storm.wordcount;

import org.apache.storm.shade.com.google.common.base.Splitter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Spliterator;
import java.util.regex.Pattern;

import static java.util.concurrent.ThreadLocalRandom.current;

/**
 * @author gx
 * @ClassName: SplitBolt
 * @Description: java类作用描述
 * @date 2019/2/11 6:30
 * @Version: 1.0
 * @since
 */
public class SplitBolt extends BaseBasicBolt {

    private static  Pattern p=Pattern.compile("\\s+");

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String line=tuple.getStringByField("line");

        Splitter.on(p).splitToList(line).forEach(s->basicOutputCollector.emit(new Values(s)));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));

    }
}
