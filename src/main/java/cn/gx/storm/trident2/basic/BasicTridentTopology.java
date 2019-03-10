package cn.gx.storm.trident2.basic;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.FailedException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author gx
 * @ClassName: BasicTridentTopology
 * @Description: java类作用描述
 * @date 2019/3/10 16:50
 * @Version: 1.0
 * @since
 */
public class BasicTridentTopology {
    private static final Logger LOG= LoggerFactory.getLogger(BasicTridentTopology.class);
    public static void main(String[] args) throws InterruptedException {
        TridentTopology tridentTopology=new TridentTopology();
        tridentTopology.newStream("text",new BasicTridentSpout(
                Arrays.asList(
                        new Values("gx1"),new Values("gx2"),
                        new Values("gx3"),new Values("gx4"),
                        new Values("gx5"),new Values("gx6"),
                        new Values("gx7"),new Values("gx8"),
                        new Values("gx9"),new Values("gx10")
                ),3,new Fields("name")
        )).localOrShuffle()
                .each(new Fields("name"),new UpperNameFunciton(),new Fields("upperName"))
                .parallelismHint(4)
                .peek(input->LOG.warn("{}========={}",input.getFields(),input));
        Config conf=new Config();
        conf.setDebug(false);
        conf.setDebug(false);
        conf.setNumWorkers(2);

        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology("BasicTridentTopology", conf, tridentTopology.build());

        System.out.println("running");
        TimeUnit.SECONDS.sleep(30);
        cluster.killTopology("BasicTridentTopology");
        cluster.shutdown();

    }

    private static class UpperNameFunciton extends BaseFunction{
        private static boolean error=true;
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

            String string = tridentTuple.getString(0);
            if (string.equals("gx7")&&error){
                error=false;
                throw new FailedException();
            }
            tridentCollector.emit(new Values(tridentTuple.getString(0).toUpperCase()));
        }
    }
}
