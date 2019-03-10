package cn.gx.storm.trident2.partitioned;

import org.apache.storm.shade.com.google.common.collect.ImmutableMap;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IPartitionedTridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class PartitionedEmitter implements IPartitionedTridentSpout.Emitter<Map<String, Integer>, Partition, MetaData>
{
    private final TopologyContext context;
    private final static Logger LOG = LoggerFactory.getLogger(PartitionedEmitter.class);
    private final static Map<String, List<Integer>> SOURCES = ImmutableMap.of(
            "1", Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            "2", Arrays.asList(10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    );

    public PartitionedEmitter(TopologyContext context)
    {
        this.context = context;
    }

    @Override
    public List<Partition> getOrderedPartitions(Map<String, Integer> allPartitionInfo)
    {
        return allPartitionInfo.keySet().stream().map(Partition::new).collect(toList());
    }

    @Override
    public MetaData emitPartitionBatchNew(TransactionAttempt tx, TridentCollector collector,
                                          Partition partition, MetaData lastMeta)
    {
        LOG.warn("txID:{},partition:{},lastMeta:{},taskID:{}", tx.getTransactionId(), partition, lastMeta, context.getThisTaskId());
        final MetaData metaData;
        if (lastMeta == null)
        {
            metaData = new MetaData(0, 5);
        } else
        {
            metaData = new MetaData(lastMeta.getEnd(), lastMeta.getEnd() + 5);
        }
        LOG.info("Create the new lastMeta:{} in taskID:{}", metaData, context.getThisTaskId());
        this.emitPartitionBatch(tx, collector, partition, metaData);
        return metaData;
    }

    @Override
    public void emitPartitionBatch(TransactionAttempt tx, TridentCollector collector,
                                   Partition partition, MetaData meta)
    {
        List<Integer> data = SOURCES.get(partition.getId());
        for (int i = meta.getStart(); i < meta.getEnd() && i < data.size(); i++)
        {
            Integer value = data.get(i);
            collector.emit(new Values(tx, value));
            LOG.warn("emit the value:{} in txID:{} by taskID:{} for partition:{}", value, tx.getTransactionId(), context.getThisTaskId(), partition);
        }
    }

    @Override
    public void refreshPartitions(List<Partition> partitionResponsibilities)
    {
        LOG.warn("taskId:{}-refresh-partitions:{}", context.getThisTaskId(), partitionResponsibilities);
    }

    @Override
    public void close()
    {

    }
}
