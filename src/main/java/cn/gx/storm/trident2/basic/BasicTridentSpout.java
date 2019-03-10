package cn.gx.storm.trident2.basic;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author gx
 * @ClassName: BasicTridentSpout
 * @Description: 123类描述
 * @date 2019/3/10 14:35
 * @Version: 1.0
 * @since
 */
public class BasicTridentSpout implements ITridentSpout<BasicTridentSpout.MetaData> {

     private final List<Values> values;
     private final int batch;
     private final Fields fields;

    public BasicTridentSpout(List<Values> values, int batch, Fields fields) {
        this.values = values;
        this.batch = batch;
        this.fields = fields;
    }

    @Override
    public BatchCoordinator<MetaData> getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return new BasicTridentCoordinator(topologyContext,batch);

    }

    @Override
    public Emitter<MetaData> getEmitter(String s, Map map, TopologyContext topologyContext) {
        return new BasicTridentEmitter(topologyContext,values);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config config=new Config();
        config.setMaxTaskParallelism(1);
        return config;
    }

    @Override
    public Fields getOutputFields() {
        List<String> realFields=new ArrayList<>();
        realFields.add("txId");
        realFields.add(this.fields.get(0));
        return new Fields(realFields);
    }

    static class BasicTridentEmitter implements ITridentSpout.Emitter<MetaData> {
        private static final Logger LOG= LoggerFactory.getLogger(BasicTridentEmitter.class);
        private final TopologyContext context;
        private final List<Values> values;

        public BasicTridentEmitter(TopologyContext context, List<Values> values) {
            this.context = context;
            this.values = values;
        }

        @Override
        public void emitBatch(TransactionAttempt transactionAttempt, MetaData metaData, TridentCollector tridentCollector) {
            for (int i=metaData.getStart();i<metaData.getEnd()&&i<values.size();i++)
            {
                List<Object> pendingValues =new ArrayList<>();
                pendingValues.add(transactionAttempt);
                pendingValues.add(values.get(i).get(0));
                tridentCollector.emit(pendingValues);
            }
        }

        @Override
        public void success(TransactionAttempt transactionAttempt) {
            LOG.warn("the txid:{} emit completed",transactionAttempt.getTransactionId());
        }

        @Override
        public void close() {

        }
    }

    static class BasicTridentCoordinator implements  ITridentSpout.BatchCoordinator<MetaData> {
        private static final Logger LOG= LoggerFactory.getLogger(BasicTridentCoordinator.class);
        private final TopologyContext context;
        private final int batch;

        public BasicTridentCoordinator(TopologyContext context, int batch) {
            this.context = context;
            this.batch = batch;
        }

        @Override
        public MetaData initializeTransaction(long txid, MetaData prev, MetaData curr) {

            LOG.warn("txID:{},prev:{},curr:{},taskId:{}",txid,prev,curr,context.getThisTaskId());
            final MetaData metaData;
            if (null==prev)
            {
                metaData=new MetaData(0,batch);
            }else {
                metaData=new MetaData(prev.getEnd(),prev.getEnd()+batch);
            }
            LOG.warn("the new metaData:{}",metaData);
            return metaData;
        }

        @Override
        public void success(long txid) {
            LOG.warn("the txId:{} process completed",txid);
        }

        @Override
        public boolean isReady(long txid) {
            return true;
        }

        @Override
        public void close() {

        }
    }

    static class MetaData implements Serializable{
        private final int start;
        private final int end;

        public MetaData(int start, int end) {
            this.start = start;
            this.end = end;
        }

        int getStart() {
            return start;
        }

        int getEnd() {
            return end;
        }
    }

}
