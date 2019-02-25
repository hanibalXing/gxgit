package cn.gx.storm.tick;

import com.esotericsoftware.minlog.Log;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author gx
 * @ClassName: NumberBolt
 * @Description: java类作用描述
 * @date 2019/2/12 19:14
 * @Version: 1.0
 * @since
 */
public class NumberBolt extends BaseBasicBolt {
    private static final Logger LOG= LoggerFactory.getLogger(NumberBolt.class);
    private List<Integer> list;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        LOG.info("bolt prepare");
        list=new ArrayList<>();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        final Config config=new Config();
        //意思是每5秒发送一个信号（具体为一个tuple,这个tuple的getSourceStreamId 是__tick）
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,5);
        return config;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        //LOG.info("source component ===={} system component id ={}",tuple.getSourceComponent(),Constants.SYSTEM_COMPONENT_ID);
        //LOG.info("source stream id ===={} system tick stream id={}",tuple.getSourceStreamId(),Constants.SYSTEM_TICK_STREAM_ID);
        LOG.info("hashcode:***********{}",this.hashCode());
        //如果是tick tuple,就可以批处理了
        if (isTick(tuple)) {
            //do something
            if  (!list.isEmpty()) {
            LOG.info("===========================");
            LOG.info("{}",list);
            LOG.info("===========================");
            list.clear();
            }
        //如果不是就把要处理的数据放在list中
        } else {
            this.list.add(tuple.getIntegerByField("number"));
            LOG.info("===========================");
            LOG.info("size ==={}",list.size());
            LOG.info("===========================");
        }
    }


    /**
     * 判断是否是tick tuple
     * @param tuple
     * @return
     */
    private boolean isTick(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)&&tuple.getSourceStreamId()
                .equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
