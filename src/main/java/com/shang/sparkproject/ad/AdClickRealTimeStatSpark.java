package com.shang.sparkproject.ad;

import com.google.common.base.Optional;
import com.shang.sparkproject.conf.ConfigurationManager;
import com.shang.sparkproject.constant.Constant;
import com.shang.sparkproject.dao.*;
import com.shang.sparkproject.dao.impl.DaoFactory;
import com.shang.sparkproject.domain.AdClickTrend;
import com.shang.sparkproject.domain.AdProvinceTop3;
import com.shang.sparkproject.domain.AdStat;
import com.shang.sparkproject.jdbc.JdbcHelper;
import com.shang.sparkproject.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

public class AdClickRealTimeStatSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AdClickRealTimeStatSpark")
                .setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put(Constant.KAFKA_METADATA_BROKER_LIST,
                ConfigurationManager.getValue(Constant.KAFKA_METADATA_BROKER_LIST));

        // 构建topic set
        String kafkaTopics = ConfigurationManager.getValue(Constant.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Set<String> topics = new HashSet<String>();
        for (String topic : kafkaTopicsSplited) {
            topics.add(topic);
        }

        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);

        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                filterByBlacklist(adRealTimeLogDStream);


        //动态生成黑名单
        generateDynamicBlacklist(filteredAdRealTimeLogDStream);

        // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
        // 最粗
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(
                filteredAdRealTimeLogDStream);

        // 业务功能二：实时统计每天每个省份top3热门广告
        // 统计的稍微细一些了
        calculateProvinceTop3Ad(adRealTimeStatDStream);


        // 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
        // 统计的非常细了
        // 我们每次都可以看到每个广告，最近一小时内，每分钟的点击量
        // 每支广告的点击趋势
        calculateAdClickCountByWindow(adRealTimeLogDStream);

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }

    /**
     * 计算最近1小时滑动窗口内的广告点击趋势
     *
     * @param adRealTimeLogDStream
     */
    private static void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {

        JavaPairDStream<String, Long> mappedRDD = adRealTimeLogDStream.mapToPair(line -> {
            String log = line._2;
            String[] logSplit = log.split(" ");
            String timeMinute = DateUtils.getDateMinFortmat(new Date(Long.valueOf(logSplit[0])));
            String adid = logSplit[4];
            String key = timeMinute + "_" + adid;
            return new Tuple2<String, Long>(key, 1L);
        });

        // 过来的每个batch rdd，都会被映射成<yyyyMMddHHMM_adid,1L>的格式
        // 每次出来一个新的batch，都要获取最近1小时内的所有的batch
        // 然后根据key进行reduceByKey操作，统计出来最近一小时内的各分钟各广告的点击次数
        // 1小时滑动窗口内的广告点击趋势
        // 点图 / 折线图
        JavaPairDStream<String, Long> reducedRDD = mappedRDD.reduceByKeyAndWindow((v1, v2) -> {
            return v1 + v2;}, Durations.minutes(60), Durations.seconds(10));

        reducedRDD.foreachRDD(rdd->{
            rdd.foreachPartition(its->{
                List<AdClickTrend> listAdClickTrend=new ArrayList<>();
                while(its.hasNext()){
                    String key= its.next()._1;
                    String[] keySplit=key.split("_");
                    long adid = Long.valueOf(keySplit[1]);
                    long clickCount = its.next()._2;

                    String timeMinute=keySplit[0];
                    String date= timeMinute.substring(0,8);
                    String hour=timeMinute.substring(8,10);
                    String minute=timeMinute.substring(10,12);

                    AdClickTrend adClickTrend=new AdClickTrend();
                    adClickTrend.setDate(date);
                    adClickTrend.setHour(hour);
                    adClickTrend.setMinute(minute);
                    adClickTrend.setAdid(adid);
                    adClickTrend.setClickCount(clickCount);

                    listAdClickTrend.add(adClickTrend);
                }
                IAdClickTrendDAO iAdClickTrendDAO= DaoFactory.getIAdClickTrendDAO();
                iAdClickTrendDAO.updateBatch(listAdClickTrend);

            });
        });


    }

    /**
     * // 业务功能二：实时统计每天每个省份top3热门广告
     *
     * @param adRealTimeStatDStream
     */
    private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform((Function<JavaPairRDD<String, Long>, JavaRDD<Row>>) rdd -> {
            JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(line -> {
                String keyLog = line._1;

                String[] keySplits = keyLog.split("_");
                String date = keySplits[0];
                String province = keySplits[1];
                long adid = Long.valueOf(keySplits[2]);

                String key = date + "_" + province + "_" + adid;
                return new Tuple2<String, Long>(key, 1L);
            });

            JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey((v1, v2) -> {
                return v1 + v2;
            });

            JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(line -> {
                String key = line._1;
                long clickCount = line._2;

                String[] keySplits = key.split("_");
                String date = keySplits[0];
                String province = keySplits[1];
                long adid = Long.valueOf(keySplits[2]);

                return RowFactory.create(date, province, adid, clickCount);

            });

            StructType schema = DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("date", DataTypes.StringType, true),
                    DataTypes.createStructField("province", DataTypes.StringType, true),
                    DataTypes.createStructField("adid", DataTypes.LongType, true),
                    DataTypes.createStructField("clickCount", DataTypes.LongType, true)
            ));


            SQLContext sqlContext = new SQLContext(rdd.context());
            DataFrame df = sqlContext.createDataFrame(rowsRDD, schema);
            df.registerTempTable("tmp_daily_ad_click_count_by_prov");

            // 使用Spark SQL执行SQL语句，配合开窗函数，统计出各身份top3热门的广告
            DataFrame provinceTop3AdDF = sqlContext.sql(
                    "select date,province,adid,clickCount" +
                            "from ( " +
                            "select date, province,adid,clickCount," +
                            "row_number() over(partition by province order by clickCount desc) rank " +
                            " from tmp_daily_ad_click_count_by_prov) t where rank >=3"
            );

            return provinceTop3AdDF.toJavaRDD();
        });

        // rowsDStream
        // 每次都是刷新出来各个省份最热门的top3广告
        // 将其中的数据批量更新到MySQL中
        rowsDStream.foreachRDD(rdd -> {
            rdd.foreachPartition(its -> {
                List<AdProvinceTop3> list = new ArrayList<>();
                while (its.hasNext()) {
                    Row row = its.next();
                    String date = row.getString(0);
                    String province = row.getString(1);
                    long adid = row.getLong(2);
                    long clickCount = row.getLong(3);

                    AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                    adProvinceTop3.setDate(date);
                    adProvinceTop3.setProvince(province);
                    adProvinceTop3.setAdid(adid);
                    adProvinceTop3.setClickCount(clickCount);
                    list.add(adProvinceTop3);
                }

                IAdProvinceTop3DAO iAdProvinceTop3DAO = DaoFactory.getAdProvinceTop3DAO();
                iAdProvinceTop3DAO.insertBash(list);


            });
        });
    }

    /**
     * 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
     *
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        JavaPairDStream<String, Long> mappedRDD = filteredAdRealTimeLogDStream.mapToPair(line -> {
            String log = line._2;
            String[] logSplits = log.split(" ");
            Date date = new Date(Long.valueOf(logSplits[0]));
            String dateKey = DateUtils.formatDateKey(date); // yyyyMMdd

            String province = logSplits[1];
            String city = logSplits[2];
            long adid = Long.valueOf(logSplits[4]);

            String key = dateKey + "_" + province + "_" + city + "_" + adid;

            return new Tuple2<String, Long>(key, 1L);

        });

        JavaPairDStream<String, Long> reduceRDD = mappedRDD.reduceByKey((v1, v2) -> {
            return v1 + v2;
        });

        //将计算的结果写入mysql
        reduceRDD.foreachRDD(rdd -> {
            rdd.foreachPartition(its -> {
                List<AdStat> adStats = new ArrayList<>();

                while (its.hasNext()) {
                    Tuple2<String, Long> log = its.next();
                    String key = log._1;
                    long clickCount = log._2;

                    String[] keySplits = key.split("_");
                    String date = keySplits[0];
                    String province = keySplits[1];
                    String city = keySplits[2];
                    long adid = Long.valueOf(keySplits[4]);

                    AdStat adStat = new AdStat();
                    adStat.setDate(date);
                    adStat.setProvince(province);
                    adStat.setCity(city);
                    adStat.setAdid(adid);
                    adStat.setClickCount(clickCount);

                    adStats.add(adStat);

                }
                IAdStatDAO iAdStatDAO = DaoFactory.getAdStatDAO();
                iAdStatDAO.insertOrUpdateBash(adStats);
            });
        });

        return reduceRDD;
    }

    // 刚刚接受到原始的用户点击行为日志之后
    // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
    // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）
    private static JavaPairDStream<String, String> filterByBlacklist(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
                (Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>) rdd -> {

                    IAdBlacklistDAO iAdBlacklistDAO = DaoFactory.getAdBlacklistDAO();
                    List<Long> useridList = iAdBlacklistDAO.findAll();

                    List<Tuple2<Long, Boolean>> tuple2list = new ArrayList<Tuple2<Long, Boolean>>();
                    for (Long userid : useridList) {
                        tuple2list.add(new Tuple2<Long, Boolean>(userid, true));
                    }

                    JavaSparkContext sc = new JavaSparkContext(rdd.context());
                    JavaPairRDD<Long, Boolean> useridRDD = sc.parallelizePairs(tuple2list);

                    // 将原始数据rdd映射成<userid, tuple2<string, string>>
                    JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(line -> {
                        String log = line._2;
                        String[] logSpits = log.split(" ");
                        long userid = Long.valueOf(logSpits[3]);
                        return new Tuple2<Long, Tuple2<String, String>>(userid, line);
                    });

                    // 将原始日志数据rdd，与黑名单rdd，进行左外连接
                    // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
                    // 用inner join，内连接，会导致数据丢失
                    JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinRDD = mappedRDD
                            .leftOuterJoin(useridRDD);

                    JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinRDD.filter(line -> {
                        Optional<Boolean> booleanOptional = line._2._2;
                        if (booleanOptional.isPresent() && booleanOptional.get()) {
                            return false;
                        }
                        return true;
                    });

                    JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(line -> {
                        return line._2._1;
                    });

                    return resultRDD;

                });

        return filteredAdRealTimeLogDStream;
    }


    /**
     * 生成动态黑名单
     *
     * @param filteredAdRealTimeLogDStream
     */
    private static void generateDynamicBlacklist(
            JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {

        // 一条一条的实时日志
        // timestamp province city userid adid
        // 某个时间点 某个省份 某个城市 某个用户 某个广告

        // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量

        // 通过对原始实时日志的处理
        // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式

        // 针对处理后的日志格式，执行reduceByKey算子即可
        // （每个batch中）每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = filteredAdRealTimeLogDStream.mapToPair(line -> {
            String log = line._2;
            String[] logSplit = log.split(",");
            // 提取出日期（yyyyMMdd）、userid、adid
            String timeStmap = logSplit[0];
            Date date = new Date(Long.valueOf(timeStmap));
            String dateKey = DateUtils.formatDateKey(date);
            String province = logSplit[1];
            long userid = Long.valueOf(logSplit[2]);
            long adid = Long.valueOf(logSplit[4]);

            // 拼接key
            String key = dateKey + "_" + province + "_" + userid + "_" + adid;
            return new Tuple2<String, Long>(key, 1L);
        }).reduceByKey((v1, v2) -> {
            return v1 + v2;
        });

        dailyUserAdClickCountDStream.foreachRDD(rdd -> {
            rdd.foreachPartition(its -> {
                List<AdStat> adStats = new ArrayList<AdStat>();
                while (its.hasNext()) {
                    Tuple2<String, Long> tuple2 = its.next();
                    String[] keySplited = tuple2._1.split("_");
                    String date = keySplited[0];
                    String province = keySplited[1];
                    String city = keySplited[2];
                    long adid = Long.valueOf(keySplited[3]);

                    long clickCount = tuple2._2;

                    AdStat adStat = new AdStat();
                    adStat.setDate(date);
                    adStat.setProvince(province);
                    adStat.setCity(city);
                    adStat.setAdid(adid);
                    adStat.setClickCount(clickCount);

                    adStats.add(adStat);
                }
                IAdStatDAO adStatDAO = DaoFactory.getAdStatDAO();
                adStatDAO.insertOrUpdateBash(adStats);

            });
        });

        // 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
        // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
        // 从mysql中查询
        // 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
        // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化

        // 对batch中的数据，去查询mysql中的点击次数，使用哪个dstream呢？
        // dailyUserAdClickCountDStream
        // 为什么用这个batch？因为这个batch是聚合过的数据，已经按照yyyyMMdd_userid_adid进行过聚合了
        // 比如原始数据可能是一个batch有一万条，聚合过后可能只有五千条
        // 所以选用这个聚合后的dstream，既可以满足咱们的需求，而且呢，还可以尽量减少要处理的数据量
        // 一石二鸟，一举两得

        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(line -> {
            String key = line._1;
            String[] keySplits = key.split("_");
            String date = keySplits[0];
            long userid = Long.valueOf(keySplits[2]);
            long adid = Long.valueOf(keySplits[4]);

            // 从mysql中查询指定日期指定用户对指定广告的点击量
            IAdUserClickCountDAO iAdUserClickCountDAO = DaoFactory.getAdUserClickCountDAO();
            int clickCount = iAdUserClickCountDAO.findClickCountByMultiKey(date, userid, adid);
            if (clickCount >= 100) {
                return true;
            }
            // 反之，如果点击量小于100的，那么就暂时不要管它了
            return false;
        });

        JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(line -> {
            String key = line._1;
            String[] keySplits = key.split("_");
            long userid = Long.valueOf(keySplits[2]);
            return userid;
        });

        // 实际上，是要通过对dstream执行操作，对其中的rdd中的userid进行全局的去重
        JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(rdd -> {
            return rdd.distinct();
        });

        // 到这一步为止，distinctBlacklistUseridDStream
        // 每一个rdd，只包含了userid，而且还进行了全局的去重，保证每一次过滤出来的黑名单用户都没有重复的
        distinctBlacklistUseridDStream.foreachRDD(rdd -> {
            rdd.foreachPartition(its -> {
                List<Long> useridList = new ArrayList<>();
                while (its.hasNext()) {
                    long userid = its.next();
                    useridList.add(userid);
                }
                IAdBlacklistDAO iAdBlacklistDAO = DaoFactory.getAdBlacklistDAO();
                iAdBlacklistDAO.insertBash(useridList);

            });
        });

    }


}
