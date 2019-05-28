package com.shang.sparkproject.page;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shang.sparkproject.conf.ConfigurationManager;
import com.shang.sparkproject.constant.Constant;
import com.shang.sparkproject.dao.ItaskDao;
import com.shang.sparkproject.dao.impl.DaoFactory;
import com.shang.sparkproject.domain.PageSplitConvertRate;
import com.shang.sparkproject.domain.Task;
import com.shang.sparkproject.util.DateUtils;
import com.shang.sparkproject.util.NumberUtils;
import com.shang.sparkproject.util.ParamUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.*;

import static com.shang.sparkproject.util.SparkUtils.mockData;

/**
 * 页面单跳转化率模块spark作业
 *
 * @author Administrator
 */

public class PageOneStepConvertRateSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("PageOneStepConvertRateSpark").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSparkSession(sc);
        mockData(sc, sqlContext);

        //从mysql中task中获取任务,首先得查询出来指定的任务，并获取任务的查询参数
        ItaskDao taskDao = DaoFactory.getTaskDao();
        long taskId = ParamUtils.getTaskIdFromMain(args);
        Task task = taskDao.findbyId(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //获取指定范围内的数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);

        //转换成javaRDD<Row>转成 javaRDD<sessionid,row>
        JavaPairRDD<String, Row> sessionid2actionRDD = actionRDD.mapToPair(row -> {
            String sessionid = null;
            if (!row.isNullAt(2)) {
                sessionid = row.getString(2);
            }
            return new Tuple2<String, Row>(sessionid, row);

        });

        sessionid2actionRDD = sessionid2actionRDD.cache();
        // 对<sessionid,访问行为> RDD，做一次groupByKey操作
        // 因为我们要拿到每个session对应的访问行为数据，才能够去生成切片
        JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();

        // 最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(
                sc, sessionid2actionsRDD, taskParam);
        Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();

        // 使用者指定的页面流是3,2,5,8,6
        // 咱们现在拿到的这个pageSplitPvMap，3->2，2->5，5->8，8->6
        long startPagePv = getStartPagePv(taskParam, sessionid2actionsRDD);

        // 计算目标页面流的各个页面切片的转化率
        Map<String, Double> convertRateMap = computePageSplitConvertRate(
                taskParam, pageSplitPvMap, startPagePv);

        // 持久化页面切片转化率
        persistConvertRate(taskId, convertRateMap);
    }


    /**
     * 持久化转化率
     *
     * @param taskId
     * @param convertRateMap
     */
    private static void persistConvertRate(
            long taskId,
            Map<String, Double> convertRateMap) {

        StringBuffer buffer = new StringBuffer();
        for (Map.Entry<String, Double> convertRate : convertRateMap.entrySet()) {
            String pageSplit = convertRate.getKey();
            double convertRat = convertRate.getValue();
            buffer.append(pageSplit + "=" + convertRat + "|");
        }
        String convertRate = buffer.toString();
        convertRate = convertRate.substring(0, convertRate.length() - 1);

        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setConvertRate(convertRate);
        pageSplitConvertRate.setTaskid(taskId);

        DaoFactory.getPageSplitConvertRateDAO().insert(pageSplitConvertRate);


    }


    /**
     * 计算页面切片转化率
     *
     * @param taskParam
     * @param pageSplitPvMap
     * @param startPagePv
     * @return
     */
    private static Map<String, Double> computePageSplitConvertRate(
            JSONObject taskParam,
            Map<String, Object> pageSplitPvMap,
            long startPagePv) {
        // 定义返回map
        Map<String, Double> convertRateMap = new HashMap<String, Double>();
        String[] targetPages = ParamUtils.getParam(taskParam, Constant.PARAM_TARGET_PAGE_FLOW).split(",");

        long lastPageSplitPv = 0L;
        // 3,5,2,4,6
        // 3_5
        // 3_5 pv / 3 pv
        // 5_2 rate = 5_2 pv / 3_5 pv

        // 通过for循环，获取目标页面流中的各个页面切片（pv）
        for (int i = 1; i <= targetPages.length; i++) {
            String targetPageSplit = targetPages[i] + "_" + targetPages[i - 1];
            long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));

            double convertRate = 0.0;
            if (i == 1) {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPv / (double) startPagePv, 2);
            } else {
                convertRate = NumberUtils.formatDouble(
                        (double) targetPageSplitPv / (double) lastPageSplitPv, 2);
            }
            convertRateMap.put(targetPageSplit, convertRate);
            lastPageSplitPv = targetPageSplitPv;
        }
        return convertRateMap;

    }


    /**
     * 获取页面流中初始页面的pv
     *
     * @param taskParam
     * @param sessionid2actionsRDD
     * @return
     */
    private static long getStartPagePv(
            JSONObject taskParam,
            JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD) {
        String targetPageFlow = ParamUtils.getParam(taskParam, Constant.PARAM_TARGET_PAGE_FLOW);
        long startPage = Long.valueOf(targetPageFlow.split(",")[0]);

        JavaRDD<Long> startPageRDD = sessionid2actionsRDD.flatMap(line -> {
            List<Long> list = new ArrayList<>();
            Iterator<Row> its = line._2.iterator();
            while (its.hasNext()) {
                Row row = its.next();
                long pageid = row.getLong(3);
                if (startPage == pageid) {
                    list.add(pageid);
                }
            }

            return list;
        });

        return startPageRDD.count();
    }

    /**
     * 页面切片生成与匹配算法
     *
     * @param sc
     * @param sessionid2actionsRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
            JavaSparkContext sc,
            JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD,
            JSONObject taskParam) {
        //获取用户提交的页面流5,2,3,4
        String targetPageFlow = ParamUtils.getParam(taskParam, Constant.PARAM_TARGET_PAGE_FLOW);
        Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);

        return sessionid2actionsRDD.flatMapToPair(line -> {

            // 定义返回list
            List<Tuple2<String, Integer>> list =
                    new ArrayList<Tuple2<String, Integer>>();

            String[] targetPages = targetPageFlowBroadcast.value().split(",");
            Iterator<Row> its = line._2.iterator();
            //对每个sessionid下访问行为按时间排序
            List<Row> rows = new ArrayList<>();
            while (its.hasNext()) {
                rows.add(its.next());
            }
            Collections.sort(rows, new Comparator<Row>() {
                @Override
                public int compare(Row o1, Row o2) {
                    String actionTime1 = o1.getString(4);
                    String actionTime2 = o2.getString(4);
                    Date date1 = DateUtils.parseTime(actionTime1);
                    Date date2 = DateUtils.parseTime(actionTime2);


                    return (int) (date1.getTime() - date2.getTime());
                }
            });

            // 页面切片的生成，以及页面流的匹配
            Long lastPageId = null;
            for (Row row : rows) {
                long pageid = row.getLong(3);
                if (lastPageId == null) {
                    lastPageId = pageid;
                    continue;
                }
                // 生成一个页面切片
                // 3,5,2,1,8,9
                // lastPageId=3
                // 5，切片，3_5
                String pageSplit = lastPageId + "_" + pageid;

                // 对这个切片判断一下，是否在用户指定的页面流中
                for (int i = 1; i <= targetPages.length; i++) {
                    String targetPageSplit = targetPages[i] + "_" + targetPages[i - 1];
                    if (pageSplit.equals(targetPageSplit)) {
                        list.add(new Tuple2<>(pageSplit, 1));
                        break;
                    }
                }

                lastPageId = pageid;
            }
            return list;

        });

    }

    private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constant.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constant.PARAM_END_DATE);

        String sql =
                "select * "
                        + "from user_visit_action "
                        + "where date>='" + startDate + "' "
                        + "and date<='" + endDate + "'";
        DataFrame df = sqlContext.sql(sql);
        return df.toJavaRDD();
    }

    public static SQLContext getSparkSession(JavaSparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constant.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }
}
