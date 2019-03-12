package cn.gx.storm.state;

import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

public class MyJdbcState extends JdbcState
{
    private static boolean error = true;

    protected MyJdbcState(Map map, int partitionIndex, int numPartitions, Options options)
    {
        super(map, partitionIndex, numPartitions, options);
    }

    @Override
    protected void prepare()
    {
        super.prepare();
    }

    @Override
    public void updateState(List<TridentTuple> tuples, TridentCollector collector)
    {
        if (error)
        {
            error = false;
            System.out.println("==========================================");
            throw new FailedException();
        }
        super.updateState(tuples, collector);
    }
}
