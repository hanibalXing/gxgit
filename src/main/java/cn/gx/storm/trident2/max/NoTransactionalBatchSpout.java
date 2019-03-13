package cn.gx.storm.trident2.max;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class NoTransactionalBatchSpout implements IBatchSpout
{

    private static int index = 0;

    @Override
    public void open(Map conf, TopologyContext context)
    {

    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector)
    {

        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 5; i++)
        {
            int value = index++;
            collector.emit(new Values("Storm:" + value, value));
            list.add(value);
        }
        list.stream().max(Comparator.naturalOrder()).ifPresent(System.out::println);
    }

    @Override
    public void ack(long batchId)
    {

    }

    @Override
    public void close()
    {

    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }

    @Override
    public Fields getOutputFields()
    {
        return new Fields("name", "number");
    }
}
