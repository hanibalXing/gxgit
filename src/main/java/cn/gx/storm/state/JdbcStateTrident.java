package cn.gx.storm.state;

import cn.gx.storm.trident2.basic.BasicTridentSpout;
import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static cn.gx.storm.utils.Runner.runThenStop;


public class JdbcStateTrident
{
    private final static Logger LOG = LoggerFactory.getLogger(JdbcStateTrident.class);

    private final static BasicTridentSpout spout = new BasicTridentSpout(Arrays.asList(
            new Values("gx1"), new Values("gx2"),
            new Values("gx3"), new Values("gx4"),
            new Values("gx5"), new Values("gx6"),
            new Values("gx7"), new Values("gx8"),
            new Values("gx9"), new Values("gx10")
    ), 5, new Fields("name"));


    public static void main(String[] args) throws InterruptedException
    {
        Map<String, Object> hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost:3306/storm");
        hikariConfigMap.put("dataSource.user", "root");
        hikariConfigMap.put("dataSource.password", "gx1984");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        final String tableName = "user";
        JdbcMapper jdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);


        JdbcState.Options options = new JdbcState.Options()
                .withConnectionProvider(connectionProvider)
                .withMapper(jdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);
        MyJdbcStateFactory jdbcStateFactory = new MyJdbcStateFactory(options);

        TridentTopology trident = new TridentTopology();
        trident.newStream("test", spout).parallelismHint(1).localOrShuffle()
                .each(new Fields("name"), new EnrichmentFunction(), new Fields("user_id", "user_name", "dept_name", "create_date"))
                .parallelismHint(3)
                .localOrShuffle()
                //.project(new Fields("user_id", "user_name", "dept_name", "create_date"))
                //.parallelismHint(3)
                .partitionPersist(jdbcStateFactory, new Fields("user_id", "user_name", "dept_name", "create_date"), new MyJdbcUpdater())
                .parallelismHint(1);
        //.peek(input -> LOG.warn("{}-{}", input.getFields(), input));

        final Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(10);
        runThenStop("jdbc", config, trident.build(), 1, TimeUnit.MINUTES);
    }

    private static class EnrichmentFunction extends BaseFunction
    {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector)
        {
            String name = tuple.getString(0);
            collector.emit(new Values(name.length(), "U-" + name, "D-" + name, System.currentTimeMillis()));
        }
    }
}
