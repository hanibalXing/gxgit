package cn.gx.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

import static cn.gx.storm.tick.Data.integer;

/**
 * @author gx
 * @ClassName: HelloDrpcClient
 * @Description: java类作用描述
 * @date 2019/2/19 16:20
 * @Version: 1.0
 * @since
 */
public class HelloDrpcClient {
	private static final Logger LOG=LoggerFactory.getLogger(HelloDrpcClient.class);

	public static void main(String[] args) throws TException {
		Config conf = new Config();
		conf.setDebug(false);
		conf.put("storm.thrift.transport", "org.apache.storm.security.auth.plain.PlainSaslTransportPlugin");
		conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
		conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
		conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);

		DRPCClient client = new DRPCClient(conf, "node1",3777);
		//String result = client.execute("echo", "gx");
		String result = client.execute("add", "1,2,4,5,6,7,9");
		LOG.warn("result :{}",result);


	}
}
