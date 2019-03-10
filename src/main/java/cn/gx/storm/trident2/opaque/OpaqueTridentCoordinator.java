package cn.gx.storm.trident2.opaque;

import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;

import java.util.Map;

public class OpaqueTridentCoordinator implements IOpaquePartitionedTridentSpout.Coordinator<Map<String, Integer>>
{
    @Override
    public boolean isReady(long txid)
    {
        return true;
    }

    @Override
    public Map<String, Integer> getPartitionsForBatch()
    {
        return ImmutableMap.of("1", 1, "2", 2);
    }

    @Override
    public void close()
    {
    }
}
