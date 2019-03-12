package cn.gx.storm.trident2.opaque;

import cn.gx.storm.trident2.partitioned.MetaData;
import cn.gx.storm.trident2.partitioned.Partition;
import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IOpaquePartitionedTridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class OpaqueTridentEmitter implements IOpaquePartitionedTridentSpout.Emitter<Map<String, Integer>, Partition, MetaData>
{
    private final static Logger LOG = LoggerFactory.getLogger(OpaqueTridentEmitter.class);

    private final TopologyContext context;

    private final static Map<String, List<Integer>> SOURCES = ImmutableMap.of(
            "1", Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            "2", Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    );

    public OpaqueTridentEmitter(TopologyContext context)
    {
        this.context = context;
    }

    @Override
    public MetaData emitPartitionBatch(TransactionAttempt tx, TridentCollector collector,
                                       Partition partition, MetaData lastMeta)
    {
        new ArrayList<>();
        LOG.warn("txID:{},partition:{},lastMeta:{},taskID:{}", tx.getTransactionId(), partition, lastMeta, context.getThisTaskId());
        final MetaData metaData;
        if (lastMeta == null)
        {
            metaData = new MetaData(0, 3);
        } else
        {
            metaData = new MetaData(lastMeta.getEnd(), lastMeta.getEnd() + 3);
        }
        LOG.info("Create the new lastMeta:{} in taskID:{}", metaData, context.getThisTaskId());
        List<Integer> data = SOURCES.get(partition.getId());
        for (int i = metaData.getStart(); i < metaData.getEnd() && i < data.size(); i++)
        {
            Integer value = data.get(i);
            collector.emit(new Values(tx, value));
            LOG.warn("emit the tuple:{} for partition:{} in taskID:{}", value, partition, context.getThisTaskId());
        }
        return metaData;
    }

    @Override
    public void refreshPartitions(List<Partition> partitionResponsibilities)
    {
        LOG.warn("==refreshPartitions=={}", partitionResponsibilities);
    }

    @Override
    public List<Partition> getOrderedPartitions(Map<String, Integer> allPartitionInfo)
    {
        LOG.warn("==getOrderedPartitions=={}", allPartitionInfo);
        return allPartitionInfo.keySet().stream().map(Partition::new).collect(toList());
    }

    @Override
    public List<Partition> getPartitionsForTask(int taskId, int numTasks,
                                                Map<String, Integer> allPartitionInfo)
    {
        LOG.warn("==getPartitionsForTask==taskId:{},numTasks:{},allPartitionInfo:{}", taskId, numTasks, allPartitionInfo);
        return allPartitionInfo.keySet().stream().map(Partition::new).collect(toList());
    }

    @Override
    public void close()
    {

    }
}
