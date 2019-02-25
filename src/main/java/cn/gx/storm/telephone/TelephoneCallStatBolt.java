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
import java.util.Objects;

/**
 * @author gx
 * @ClassName: TelephoneCallStatBolt
 * @Description: java类作用描述
 * @date 2019/2/11 5:56
 * @Version: 1.0
 * @since
 */
public class TelephoneCallStatBolt extends BaseBasicBolt {
    private static final Logger LOG= LoggerFactory.getLogger(TelephoneCallTimeBolt.class);
    private Map<Call,Integer> map;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        map=new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String caller=tuple.getStringByField("caller");
        String callee=tuple.getStringByField("callee");
        Call call=new Call(caller,callee);
        if (map.containsKey(call)) {
            map.put(call,map.get(call)+1);
        } else {
            map.put(call,1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        map.forEach((call,stats)->{
            LOG.info("call relationship {} =====times-------->{}",call,stats);
        });
    }

    private static class Call {
        private String caller;
        private String callee;

        public Call(String caller, String callee) {
            this.caller = caller;
            this.callee = callee;
        }

        @Override
        public String toString() {
            return "Call{" +
                    "caller='" + caller + '\'' +
                    ", callee='" + callee + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Call call = (Call) o;
            return Objects.equals(caller, call.caller) &&
                    Objects.equals(callee, call.callee);
        }

        @Override
        public int hashCode() {

            return Objects.hash(caller, callee);
        }
    }
}
