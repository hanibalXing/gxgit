package cn.gx.storm.state;

import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

public class MyJdbcStateFactory implements StateFactory
{
    private JdbcState.Options options;

    public MyJdbcStateFactory(JdbcState.Options options)
    {
        this.options = options;
    }

    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int partitionIndex, int numPartitions)
    {
        MyJdbcState state = new MyJdbcState(map, partitionIndex, numPartitions, options);
        state.prepare();
        return state;
    }
}
