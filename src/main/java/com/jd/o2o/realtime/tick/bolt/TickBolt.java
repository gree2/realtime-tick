package com.jd.o2o.realtime.tick.bolt;


import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * TickBolt
 *
 * User: houqiangliang
 * Date: 2016-09-22
 * Time: 08:57:38
 */
public class TickBolt extends BaseBasicBolt {

    // 日志
    private final static Logger LOGGER = LoggerFactory.getLogger(TickBolt.class);

    /**
     * 格式化日期(2016-02-03 12:23:34)
     *
     * @param date 时间
     * @return milliseconds
     */
    public static String dateToString(Date date) {
        return DateFormatUtils.format(date, "yyyy-MM-dd hh:mm:ss");
    }

    /**
     * bolt的初始化操作
     *
     * @param conf    配置
     * @param context 上下文
     */
    @Override
    public void prepare(Map conf, TopologyContext context) {

    }

    /**
     * 处理tuple的逻辑，来一个tuple，执行一次
     *
     * @param tuple tuple
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (isTickTuple(tuple)) {
            Calendar calendar = Calendar.getInstance();
            LOGGER.info("tick init at {}", dateToString(calendar.getTime()));
            try {
                Thread.sleep(30 * 1000);
                calendar = Calendar.getInstance();
                LOGGER.info("tick done at {}", dateToString(calendar.getTime()));
            } catch (InterruptedException e) {
                calendar = Calendar.getInstance();
                LOGGER.info("tick fail at {}", dateToString(calendar.getTime()));
                e.printStackTrace();
            }
        }
    }

    /**
     * 是周期元组
     *
     * @param tuple tuple
     * @return true/false
     */
    public static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
                tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    /**
     * @return map
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        // 每秒执行一次
        final int secs = 1;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, secs);
        return conf;
    }

    /**
     * @param declarer 声明
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}