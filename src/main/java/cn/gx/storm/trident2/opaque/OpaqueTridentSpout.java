package cn.gx.storm.trident2.opaque;

import cn.gx.storm.trident2.partitioned.MetaData;
import cn.gx.storm.trident2.partitioned.Partition;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class OpaqueTridentSpout implements IOpaquePartitionedTridentSpout<Map<String, Integer>, Partition, MetaData>
{
    @Override
    public Emitter<Map<String, Integer>, Partition, MetaData> getEmitter(Map conf, TopologyContext context)
    {
        return new OpaqueTridentEmitter(context);
    }

    @Override
    public Coordinator<Map<String, Integer>> getCoordinator(Map conf, TopologyContext context)
    {
        return new OpaqueTridentCoordinator();
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }

    @Override
    public Fields getOutputFields()
    {
        return new Fields("txId", "number");
    }
}
