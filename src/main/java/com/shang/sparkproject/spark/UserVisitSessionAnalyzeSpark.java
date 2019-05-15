package com.shang.sparkproject.spark;

import com.alibaba.fastjson.JSONObject;
import com.shang.sparkproject.conf.ConfigurationManager;
import com.shang.sparkproject.constant.Constant;
import com.shang.sparkproject.dao.ISessionAggrStatDAO;
import com.shang.sparkproject.dao.IgetSessionRandomExtractDAO;
import com.shang.sparkproject.dao.ItaskDao;
import com.shang.sparkproject.dao.impl.DaoFactory;
import com.shang.sparkproject.domain.SessionAggrStat;
import com.shang.sparkproject.domain.SessionDetail;
import com.shang.sparkproject.domain.SessionRandomExtract;
import com.shang.sparkproject.domain.Task;
import com.shang.sparkproject.jdbc.JdbcHelper;
import com.shang.sparkproject.test.MockDataTest;
import com.shang.sparkproject.util.*;
import javolution.util.Index;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import scala.Tuple2;

import java.util.*;


public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {
        args = new String[]{"1"};
        SparkConf conf = new SparkConf().setAppName("UserVisitSessionAnalyzeSpark")
                .setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSparkSession(sc);
        mockData(sc, sqlContext);

        //从mysql中task中获取任务,首先得查询出来指定的任务，并获取任务的查询参数
        ItaskDao taskDao = DaoFactory.getTaskDao();
        long taskId = ParamUtils.getTaskIdFromMain(args);
        Task task = taskDao.findbyId(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 如果要进行session粒度的数据聚合
        // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBysession(sqlContext, actionRDD);

        //重构，同时进行过滤和统计，添加累计器对session访问时长的占比的统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("",
                new SessionAggrStatAccumulator());

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD,
                taskParam, sessionAggrStatAccumulator);

        JavaPairRDD<String,Row> sessionid2actionRDD=getSessionid2ActionRDD(actionRDD);


        //随机抽取session，按照每个小时的平均抽取
        randomExtractSession(task.getTaskid(),filteredSessionid2AggrInfoRDD,sessionid2actionRDD);

        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),
                task.getTaskid());


        sc.close();
    }


    public static SQLContext getSparkSession(JavaSparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constant.SPARK_LOCAL);
        if (local) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constant.SPARK_LOCAL);
        if (local) {
            MockDataTest.mock(sc, sqlContext);
        }
    }

    //获取用户访问session RDD
    public static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam) {

        String startDate = ParamUtils.getParam(taskParam, Constant.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constant.PARAM_END_DATE);
        String sql = "select * from user_visit_action" +
                " where date >='" + startDate + "' and date <='" + endDate + "'";
        System.out.println(sql);
        DataFrame userVisitDF = sqlContext.sql(sql);
        System.out.println(userVisitDF.schema());
        JavaRDD<Row> userVisitRDD = userVisitDF.toJavaRDD();
        return userVisitRDD;
    }


    /**
     * 获取sessionid2到访问行为数据的映射的RDD
     * @param actionRDD
     * @return
     */
    public static JavaPairRDD<String,Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD){
        return actionRDD.mapToPair(line->{
            return new Tuple2<String,Row>(line.getString(2),line);
        });
    }

    /**
     * 对行为数据按session粒度进行聚合
     *
     * @param actionRDD 行为数据RDD
     * @return session粒度聚合数据
     */
    public static JavaPairRDD<String, String> aggregateBysession(SQLContext sqlContext, JavaRDD<Row> actionRDD) {

        JavaPairRDD<String, Row> actionRDD2SessionRDD = actionRDD.mapToPair(row -> {
            String sessionid = row.getString(2);
            return new Tuple2<String, Row>(sessionid, row);
        });

        //按照sessionid groupby聚合
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionRDD = actionRDD2SessionRDD.groupByKey();

        //对sessionid组内的keyword、点击品类做聚合 <userid 聚合值>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionRDD.mapToPair(line -> {
            String sessionid = line._1;
            Iterator<Row> iterator = line._2.iterator();
            StringBuffer searchKeywordBuffer = new StringBuffer();
            StringBuffer clickCategoryidBuffer = new StringBuffer();

            Long userid = null;
            //session的起始时间
            Date startTime = null;
            //session的结束时间
            Date endTime = null;
            //session的访问步长
            int stepLength = 0;


            while (iterator.hasNext()) {
                Row row = iterator.next();
                if (userid == null) {
                    userid = row.getLong(1);
                }
                String searchKeyword = row.getString(5);
                Long clickCategoryId = null;
                if (!row.isNullAt(6)) {
                    clickCategoryId = row.getLong(6);
                }


                if (StringUtils.isNotEmpty(searchKeyword)) {
                    if (!searchKeywordBuffer.toString().contains(searchKeyword)) {
                        searchKeywordBuffer.append(searchKeyword + ",");
                    }
                }
                if (clickCategoryId != null) {
                    if (!clickCategoryidBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                        clickCategoryidBuffer.append(clickCategoryId + ",");
                    }
                }

                Date actionTime = null;
                if ((!row.isNullAt(4)) && (!row.getString(4).equals(""))) {
                    actionTime = DateUtils.parseTime(row.getString(4));
                }
                if (startTime == null) {
                    startTime = actionTime;
                }

                if (endTime == null) {
                    endTime = actionTime;
                }
                if (actionTime != null) {
                    if (actionTime.before(startTime)) {
                        startTime = actionTime;
                    }

                    if (actionTime.after(endTime)) {
                        endTime = actionTime;
                    }
                }

                stepLength++;
            }

            String searchKeywords = StringUtils.trimComma(searchKeywordBuffer.toString());
            String clickCategoryids = StringUtils.trimComma(clickCategoryidBuffer.toString());
            //计算访问时长
            long visitLength = 0L;
            if (endTime != null && startTime != null) {
                visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
            }


            String partAggInfo = Constant.FIELD_SESSION_ID + "=" + sessionid + "|"
                    + Constant.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                    + Constant.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryids + "|"
                    + Constant.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                    + Constant.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                    + Constant.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

            return new Tuple2<Long, String>(userid, partAggInfo);

        });

        //查询所有用户数据，并映射成<userid,Row>的格式
        String sql = "select * from user_info";
        DataFrame userRDD = sqlContext.sql(sql);
        JavaPairRDD<Long, Row> userid2InfoRDD = userRDD.toJavaRDD()
                .mapToPair(row -> {
                    return new Tuple2<Long, Row>(row.getLong(0), row);
                });

        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(line -> {
            String partAggrInfo = line._2._1;
            Row userRow = line._2._2;

            String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,
                    "\\|", Constant.FIELD_SESSION_ID);
            int age = userRow.getInt(3);
            String professional = userRow.getString(4);
            String city = userRow.getString(5);
            String sex = userRow.getString(6);

            String fullAggrInfo = partAggrInfo + "|"
                    + Constant.FIELD_AGE + "=" + age + "|"
                    + Constant.FIELD_PROFESSIONAL + "=" + professional + "|"
                    + Constant.FIELD_CITY + "=" + city + "|"
                    + Constant.FIELD_SEX + "=" + sex;
            return new Tuple2<String, String>(sessionid, fullAggrInfo);

        });
        return sessionid2FullAggrInfoRDD;

    }


    /**
     * 过滤session数据
     *
     * @param sessionid2AggrInfoRDD
     * @return
     */

    public static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            JSONObject taskParam, Accumulator<String> sessionAggrStatAccumulator) {

        String startAge = ParamUtils.getParam(taskParam, Constant.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constant.PARAM_END_AGE);
        String professionals = ParamUtils.getParam(taskParam, Constant.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constant.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constant.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constant.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constant.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constant.PARAM_START_AGE + "=" + startAge : "") + "|"
                + (endAge != null ? Constant.PARAM_END_AGE + "=" + endAge : "") + "|"
                + (professionals != null ? Constant.PARAM_PROFESSIONALS + "=" + professionals : "") + "|"
                + (cities != null ? Constant.PARAM_CITIES + "=" + cities : "") + "|"
                + (sex != null ? Constant.PARAM_SEX + "=" + sex : "") + "|"
                + (keywords != null ? Constant.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constant.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
        if (_parameter.endsWith("//|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(line -> {
            String agginfo = line._2;
            // 接着，依次按照筛选条件进行过滤
            // 按照年龄范围进行过滤（startAge、endAge）
            if (!ValidUtils.between(agginfo, Constant.FIELD_AGE, parameter,
                    Constant.PARAM_START_AGE, Constant.PARAM_END_AGE)) {
                return false;
            }

            // 按照职业范围进行过滤（professionals）
            // 互联网,IT,软件
            // 互联网
            if (!ValidUtils.in(agginfo, Constant.FIELD_PROFESSIONAL, parameter,
                    Constant.PARAM_PROFESSIONALS)) {
                return false;
            }


            // 按照城市范围进行过滤（cities）
            // 北京,上海,广州,深圳
            // 成都
            if (!ValidUtils.in(agginfo, Constant.FIELD_CITY, parameter,
                    Constant.PARAM_CITIES)) {
                return false;
            }

            // 按照性别进行过滤
            // 男/女
            // 男，女
            if (!ValidUtils.equal(agginfo, Constant.FIELD_SEX, parameter,
                    Constant.PARAM_SEX)) {
                return false;
            }

            // 按照搜索词进行过滤
            // 我们的session可能搜索了 火锅,蛋糕,烧烤
            // 我们的筛选条件可能是 火锅,串串香,iphone手机
            // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
            // 任何一个搜索词相当，即通过
            if (!ValidUtils.in(agginfo, Constant.FIELD_SEARCH_KEYWORDS, parameter,
                    Constant.PARAM_KEYWORDS)) {
                return false;
            }

            // 按照点击品类id进行过滤

            if (!ValidUtils.in(agginfo, Constant.FIELD_CLICK_CATEGORY_IDS, parameter,
                    Constant.PARAM_CATEGORY_IDS)) {
                return false;
            }
            // 如果经过了之前的多个过滤条件之后，程序能够走到这里
            // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
            // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
            // 进行相应的累加计数

            // 主要走到这一步，那么就是需要计数的session
            sessionAggrStatAccumulator.add(Constant.SESSION_COUNT);
            long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(agginfo, "\\|",
                    Constant.FIELD_VISIT_LENGTH));
            long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(agginfo, "\\|",
                    Constant.FIELD_STEP_LENGTH));

            calculateVisitLength(visitLength, sessionAggrStatAccumulator);
            calculateStepLength(stepLength, sessionAggrStatAccumulator);

            return true;
        });


        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * 随机抽取session
     *
     * @param taskid
     * @param filteredSessionid2AggrInfoRDD
     */

    private static void randomExtractSession(long taskid,
                                             JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
                                             JavaPairRDD<String,Row> sessionid2actionRDD ) {

        // 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
        JavaPairRDD<String, String> time2sessionidRDD = filteredSessionid2AggrInfoRDD.mapToPair(line -> {
            String aggInfo = line._2;
            String startTime = StringUtils.getFieldFromConcatString(aggInfo, "\\|", Constant.FIELD_START_TIME);
            String dateHour = DateUtils.getDateHour(startTime);
            return new Tuple2(dateHour, aggInfo);
        });

        // 得到每天每小时的session数量
        Map<String, Object> countMap = time2sessionidRDD.countByKey();

        /**
         * 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
         */

        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap = new java.util.HashMap<String, Map<String, Long>>();
        for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new java.util.HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }
            hourCountMap.put(hour, count);
        }

        // 开始实现我们的按时间比例随机抽取算法

        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        // <date,<hour,(3,5,20,102)>>
        Map<String, Map<String, List<Integer>>> dateHourExtractMap =
                new HashMap<String, Map<String, List<Integer>>>();

        Random random = new Random();

        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算出这一天的session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                int hourExtractNumber = (int) (((double) count / (double) sessionCount) * extractNumberPerDay);
                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                List<Integer> extractIndexList = hourExtractMap.get(hour);
                // 先获取当前小时的存放随机数的list
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }
                // 生成上面计算出来的数量的随机数
                for (int i = 0; i <= hourExtractNumber; i++) {
                    int index = random.nextInt(hourExtractNumber);
                    while (extractIndexList.contains(index)) {
                        index = random.nextInt((int) hourExtractNumber);
                    }
                    extractIndexList.add(index);
                }
            }
        }

        /**
         * 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
         */

        // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

        // 我们用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
        // 然后呢，会遍历每天每小时的session
        // 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
        // 那么抽取该session，直接写入MySQL的random_extract_session表
        // 将抽取出来的session id返回回来，形成一个新的JavaRDD<String>
        // 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表

        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(line -> {
            List<Tuple2<String, String>> extractSessionids =
                    new ArrayList<Tuple2<String, String>>();
            String dateHour = line._1;
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            Iterator<String> iterator = line._2.iterator();

            List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
            IgetSessionRandomExtractDAO getSessionRandomExtractDAO = DaoFactory.getSessionRandomExtractDAO();

            int index = 0;
            while (iterator.hasNext()) {
                String sessionAggrInfo = iterator.next();
                if (extractIndexList.contains(index)) {
                    String sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|",
                            Constant.FIELD_SESSION_ID);
                    SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                    sessionRandomExtract.setTaskid(taskid);
                    sessionRandomExtract.setSessionid(sessionid);
                    sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo,
                            "\\|", Constant.FIELD_START_TIME));
                    sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|",
                            Constant.FIELD_SEARCH_KEYWORDS));
                    sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|",
                            Constant.FIELD_CLICK_CATEGORY_IDS));
                    getSessionRandomExtractDAO.insert(sessionRandomExtract);
                    extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
                }
                index++;
            }
            return extractSessionids;
        });

        /**
         * 第四步：获取抽取出来的session的明细数据
         */
        JavaPairRDD<String,Tuple2<String,Row>> extractSessionDetailRDD=extractSessionidsRDD.join(sessionid2actionRDD);
        extractSessionDetailRDD.foreach(line->{
            Row row=line._2._2;

            SessionDetail sessionDetail = new SessionDetail();
            sessionDetail.setTaskid(taskid);
            sessionDetail.setUserid(row.getLong(0));
            sessionDetail.setSessionid(row.getString(1));
            sessionDetail.setPageid(row.getLong(2));
            sessionDetail.setActionTime(row.getString(3));
            sessionDetail.setSearchKeyword(row.getString(4));
            sessionDetail.setClickCategoryId(row.getLong(5));
            sessionDetail.setClickProductId(row.getLong(6));
            sessionDetail.setOrderCategoryIds(row.getString(7));
            sessionDetail.setOrderProductIds(row.getString(8));
            sessionDetail.setPayCategoryIds(row.getString(9));
            sessionDetail.setPayProductIds(row.getString(11));

            DaoFactory.getSessionDetailDAO().insert(sessionDetail);
        });
    }


    /**
     * 计算访问时长范围
     *
     * @param visitLength
     */
    public static void calculateVisitLength(long visitLength, Accumulator<String> sessionAggrStatAccumulator) {
        if (visitLength >= 1 && visitLength <= 3) {
            sessionAggrStatAccumulator.add(Constant.TIME_PERIOD_1s_3s);
        } else if (visitLength >= 4 && visitLength <= 6) {
            sessionAggrStatAccumulator.add(Constant.TIME_PERIOD_4s_6s);
        } else if (visitLength >= 7 && visitLength <= 9) {
            sessionAggrStatAccumulator.add(Constant.TIME_PERIOD_7s_9s);
        } else if (visitLength >= 10 && visitLength <= 30) {
            sessionAggrStatAccumulator.add(Constant.TIME_PERIOD_10s_30s);
        } else if (visitLength > 30 && visitLength <= 60) {
            sessionAggrStatAccumulator.add(Constant.TIME_PERIOD_30s_60s);
        } else if (visitLength > 60 && visitLength <= 180) {
            sessionAggrStatAccumulator.add(Constant.TIME_PERIOD_1m_3m);
        } else if (visitLength > 180 && visitLength <= 600) {
            sessionAggrStatAccumulator.add(Constant.TIME_PERIOD_3m_10m);
        } else if (visitLength > 600 && visitLength <= 1800) {
            sessionAggrStatAccumulator.add(Constant.TIME_PERIOD_10m_30m);
        } else if (visitLength > 1800) {
            sessionAggrStatAccumulator.add(Constant.TIME_PERIOD_30m);
        }
    }

    ;

    /**
     * 计算访问步长范围
     *
     * @param stepLength
     */
    private static void calculateStepLength(long stepLength, Accumulator<String> sessionAggrStatAccumulator) {
        if (stepLength >= 1 && stepLength <= 3) {
            sessionAggrStatAccumulator.add(Constant.STEP_PERIOD_1_3);
        } else if (stepLength >= 4 && stepLength <= 6) {
            sessionAggrStatAccumulator.add(Constant.STEP_PERIOD_4_6);
        } else if (stepLength >= 7 && stepLength <= 9) {
            sessionAggrStatAccumulator.add(Constant.STEP_PERIOD_7_9);
        } else if (stepLength >= 10 && stepLength <= 30) {
            sessionAggrStatAccumulator.add(Constant.STEP_PERIOD_10_30);
        } else if (stepLength > 30 && stepLength <= 60) {
            sessionAggrStatAccumulator.add(Constant.STEP_PERIOD_30_60);
        } else if (stepLength > 60) {
            sessionAggrStatAccumulator.add(Constant.STEP_PERIOD_60);
        }
    }

    /**
     * 计算各session范围占比，并写入MySQL
     *
     * @param value
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|",
                Constant.SESSION_COUNT));
        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constant.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);
        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DaoFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }


}
