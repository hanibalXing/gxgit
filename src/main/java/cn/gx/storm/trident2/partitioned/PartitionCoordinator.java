package cn.gx.storm.trident2.partitioned;

import org.apache.storm.trident.spout.IPartitionedTridentSpout;

import java.util.HashMap;
import java.util.Map;

public class PartitionCoordinator implements IPartitionedTridentSpout.Coordinator<Map<String, Integer>>
{
    @Override
    public Map<String, Integer> getPartitionsForBatch()
    {
        Map<String, Integer> map = new HashMap<>();
        map.put("1", 1);
        map.put("2", 2);
        return map;
    }

    @Override
    public boolean isReady(long txid)
    {
        return true;
    }

    @Override
    public void close()
    {

    }
}