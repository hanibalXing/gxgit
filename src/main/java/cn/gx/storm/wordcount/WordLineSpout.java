package cn.gx.storm.wordcount;

import clojure.lang.IFn;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @author gx
 * @ClassName: WordLineSpout
 * @Description: java类作用描述
 * @date 2019/2/11 6:24
 * @Version: 1.0
 * @since
 */
public class WordLineSpout extends BaseRichSpout {
    private static final Logger LOG= LoggerFactory.getLogger(WordLineSpout.class);
    private List<String> wordList;
    private SpoutOutputCollector collector;
    private int index;
    private HashMap<String,String> resultMap;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        wordList= ImmutableList.of(
                "xx gxx gxg fdas wj fdsa",
                "xx java gxg fdas spark fdsa",
                "xx hadoop gxg fdas fdsasd fdsa",
                "xx java gxg fdas fdsasd spark",
                "xx java gxg gzh wj spark"
                );
        this.collector=spoutOutputCollector;
        resultMap=new HashMap<>();
    }

    @Override
    public void nextTuple() {
        while (index<wordList.size()) {
            String line=wordList.get(index++);
            String messageId=UUID.randomUUID().toString();
            collector.emit(new Values(line),messageId );
            resultMap.put(messageId,line);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));

    }

    @Override
    public void ack(Object msgId) {
        LOG.info("message successful id is ==========={}",msgId.toString());
        resultMap.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        LOG.info("message fail id is ==========={}",msgId.toString());
        collector.emit(new Values(resultMap.get(msgId)),msgId );
    }
}
