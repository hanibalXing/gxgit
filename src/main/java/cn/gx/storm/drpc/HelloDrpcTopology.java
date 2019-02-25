package cn.gx.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author gx
 * @ClassName: HelloDrpcTopology
 * @Description: java类作用描述
 * @date 2019/2/19 14:53
 * @Version: 1.0
 * @since
 */
public class HelloDrpcTopology {
	private static final Logger LOG=LoggerFactory.getLogger(HelloDrpcTopology.class);

	public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

		boolean isRemote = false;
		if (args.length > 0) {
			isRemote = true;
		}
		final LinearDRPCTopologyBuilder builder=new LinearDRPCTopologyBuilder("echo");
		builder.addBolt(new EchoBolt(),1);
		Config conf=new Config();
		conf.setDebug(false);
		conf.setNumWorkers(2);
		if (isRemote) {
			StormSubmitter.submitTopologyWithProgressBar("HelloDrpcTopology",conf,builder.createRemoteTopology());
		}else {

			LocalDRPC drpc=new LocalDRPC();
			LocalCluster cluster=new LocalCluster();
			cluster.submitTopology("HelloDrpcTopology",conf,builder.createLocalTopology(drpc));
			Arrays.asList("gx","xg","wj","jw").forEach(item->{
				String echo = drpc.execute("echo", item);
				LOG.warn("result is: {}",echo);
			});
			cluster.killTopology("HelloDrpcTopology");
			cluster.shutdown();
			drpc.shutdown();
		}

	}

	public static class EchoBolt extends BaseBasicBolt{

		@Override
		public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
			LOG.warn("tuple:{}",tuple.getFields());
			Object id = tuple.getValue(0);
			String name=tuple.getString(1);
			basicOutputCollector.emit(new Values(id,"echo:" + name));

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
			outputFieldsDeclarer.declare(new Fields("id","value"));
		}
	}
}
