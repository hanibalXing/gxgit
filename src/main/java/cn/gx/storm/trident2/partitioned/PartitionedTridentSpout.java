package cn.gx.storm.trident2.partitioned;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class PartitionedTridentSpout implements IPartitionedTridentSpout<Map<String, Integer>, Partition, MetaData>
{
    @Override
    public Coordinator<Map<String, Integer>> getCoordinator(Map conf, TopologyContext context)
    {
        return new PartitionCoordinator();
    }

    @Override
    public Emitter<Map<String, Integer>, Partition, MetaData> getEmitter(Map conf, TopologyContext context)
    {
        return new PartitionedEmitter(context);
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
