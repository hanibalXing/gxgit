package cn.gx.storm.trident;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author gx
 * @ClassName: MaxTaskIdGrouping
 * @Description: java类作用描述
 * @date 2019/2/14 2:36
 * @Version: 1.0
 * @since
 */
public class MaxTaskIdGrouping implements CustomStreamGrouping {
    private static final Logger LOG= LoggerFactory.getLogger(MaxTaskIdGrouping.class);
    private int taskId;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        LOG.info("tasks ids =========== {}",list);
        taskId=list.stream().max(((o1, o2) -> o1.compareTo(o2))).get();
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
       // LOG.info("choosetasks list {}",list.get(0).getClass());
        return Arrays.asList(taskId);
    }
}
