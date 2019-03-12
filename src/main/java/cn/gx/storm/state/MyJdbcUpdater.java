package cn.gx.storm.state;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;

public class MyJdbcUpdater extends BaseStateUpdater<MyJdbcState>
{
    @Override
    public void updateState(MyJdbcState jdbcState, List<TridentTuple> tuples, TridentCollector collector)
    {
        jdbcState.updateState(tuples, collector);
    }
}
