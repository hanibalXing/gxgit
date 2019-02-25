package cn.gx.storm.telephone;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author gx
 * @ClassName: TelephoneCallTimeBolt
 * @Description: java类作用描述
 * @date 2019/2/11 5:36
 * @Version: 1.0
 * @since
 */
public class TelephoneCallTimeBolt extends BaseBasicBolt {
    private static final Logger LOG=LoggerFactory.getLogger(TelephoneCallTimeBolt.class);
    private Map<String,Double> map;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        map=new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String caller=tuple.getStringByField("caller");
        Double callTime=tuple.getDoubleByField("callTime");
        if (map.containsKey(caller)) {
            map.put(caller,map.get(caller)+callTime);
        } else {
            map.put(caller,callTime);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        map.forEach((caller,callTime)->{
            LOG.info("call is============= {} and call time is =========={}",caller,callTime);
        });
    }
}
