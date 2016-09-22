package com.jd.o2o.realtime.tick.topology;


import com.jd.o2o.realtime.tick.bolt.TickBolt;
import com.jd.o2o.realtime.tick.bolt.TickSleepBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * TickTopology
 *
 * User: houqiangliang
 * Date: 2016-09-22
 * Time: 08:55:07
 */
public class TickTopology {

    // com.jd.o2o.realtime.tick bolt
    private static final String TICK_BOLT = "TICK_BOLT";
    private static final String TICK_SLEEP_BOLT = "TICK_SLEEP_BOLT";

    /**
     * 程序入口
     *
     * @param args 参数列表
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String id = "tick";
        TopologyBuilder builder = new TopologyBuilder();

        builder.setBolt(TICK_BOLT, new TickBolt(), 1);
        builder.setBolt(TICK_SLEEP_BOLT, new TickSleepBolt(), 1);

        Config stormConf = new Config();
//        stormConf.put(Config.NIMBUS_HOST, "127.0.0.1");
        stormConf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 50000);
        stormConf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32);
        stormConf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384);
        stormConf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384);
        stormConf.setNumWorkers(1);
        stormConf.setNumAckers(1);

        boolean localMode = args.length > 1 && "local".equals(args[1]);
        if (localMode) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(id + "_topology", stormConf, builder.createTopology());
        } else {
            try {
                StormSubmitter.submitTopology(id + "_topology", stormConf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }
    }
}
