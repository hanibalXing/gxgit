package cn.gx.storm.wordcount;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.concurrent.ThreadLocalRandom.current;
import static java.util.stream.Collectors.toList;

/**
 * @author gx
 * @ClassName: CountBolt
 * @Description: java类作用描述
 * @date 2019/2/11 6:35
 * @Version: 1.0
 * @since
 */
public class CountBolt extends BaseBasicBolt {
    private static final Logger LOG= LoggerFactory.getLogger(CountBolt.class);
    private Map<String,Integer> wcMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        wcMap=new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word=tuple.getStringByField("word");
        if (wcMap.containsKey(word)) {
            wcMap.put(word,wcMap.get(word)+1);
        } else {
            wcMap.put(word,1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
         wcMap.entrySet().stream()
                 .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                 .collect(toList())
                 .forEach(e->{
                     LOG.info("word--------------{} appares---------{} times",e.getKey(), e.getValue());
                 });


    }
}
