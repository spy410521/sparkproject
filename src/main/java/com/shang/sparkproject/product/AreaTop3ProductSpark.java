package com.shang.sparkproject.product;

import com.alibaba.fastjson.JSONObject;
import com.shang.sparkproject.constant.Constant;
import com.shang.sparkproject.dao.IAreaTop3ProductDAO;
import com.shang.sparkproject.dao.ItaskDao;
import com.shang.sparkproject.dao.impl.DaoFactory;
import com.shang.sparkproject.domain.AreaTop3Product;
import com.shang.sparkproject.domain.Task;
import com.shang.sparkproject.util.ParamUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.shang.sparkproject.util.SparkUtils.mockData;

public class AreaTop3ProductSpark {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AreaTop3ProductSpark")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 注册自定义函数
        sqlContext.udf().register("concat_long_string",
                new ConcatLongStringUDF(), DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct",
                new GroupConcatDistinctUDAF());
        sqlContext.udf().register("get_json_object",
                new GetJsonObjectUDF(), DataTypes.StringType);

        // 准备模拟数据
        mockData(sc, sqlContext);

        //从mysql中task中获取任务,首先得查询出来指定的任务，并获取任务的查询参数
        ItaskDao taskDao = DaoFactory.getTaskDao();
        long taskId = ParamUtils.getTaskIdFromMain(args);
        Task task = taskDao.findbyId(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParam(taskParam, Constant.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constant.PARAM_END_DATE);

        // 查询用户指定日期范围内的点击行为数据
        JavaPairRDD<Long, Row> clickActionRDD = getClickActionRDDByDate(
                sqlContext, startDate, endDate);

        // 从MySQL中查询城市信息
        JavaPairRDD<Long, Row> cityInfoRDD = getcityid2CityInfoRDD(sqlContext);

        // 生成点击商品基础信息临时表
        // 技术点3：将RDD转换为DataFrame，并注册临时表
        generateTempClickProductBasicTable(sqlContext,
                clickActionRDD, cityInfoRDD);

        // 生成各区域各商品点击次数的临时表
        generateTempAreaPrdocutClickCountTable(sqlContext);

        // 生成包含完整商品信息的各区域各商品点击次数的临时表
        generateTempAreaFullProductClickCountTable(sqlContext);

        // 使用开窗函数获取各个区域内点击次数排名前3的热门商品
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);
        System.out.println("areaTop3ProductRDD: " + areaTop3ProductRDD.count());

        // 这边的写入mysql和之前不太一样
        // 因为实际上，就这个业务需求而言，计算出来的最终数据量是比较小的
        // 总共就不到10个区域，每个区域还是top3热门商品，总共最后数据量也就是几十个
        // 所以可以直接将数据collect()到本地
        // 用批量插入的方式，一次性插入mysql即可
        List<Row> rows = areaTop3ProductRDD.collect();
        System.out.println("rows: " + rows.size());
        persistAreaTop3Product(taskId, rows);


        sc.close();
    }

    /**
     * 将计算出来的各区域top3热门商品写入MySQL中
     * @param rows
     */
    private static void persistAreaTop3Product(long taskId, List<Row> rows) {
        List<AreaTop3Product> list=new ArrayList<AreaTop3Product>();

        for(Row row:rows){
            AreaTop3Product areaTop3Product=new AreaTop3Product();
            areaTop3Product.setTaskid(taskId);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductid(row.getLong(2));
            areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));
            list.add(areaTop3Product);
        }
        IAreaTop3ProductDAO iAreaTop3ProductDAO= DaoFactory.getAreaTop3ProductDAO();
        iAreaTop3ProductDAO.insertBash(list);
    }

    /**
     * 获取各区域top3热门商品
     * @param sqlContext
     * @return
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {
        // 技术点：开窗函数

        // 使用开窗函数先进行一个子查询
        // 按照area进行分组，给每个分组内的数据，按照点击次数降序排序，打上一个组内的行号
        // 接着在外层查询中，过滤出各个组内的行号排名前3的数据
        // 其实就是咱们的各个区域下top3热门商品

        // 华北、华东、华南、华中、西北、西南、东北
        // A级：华北、华东
        // B级：华南、华中
        // C级：西北、西南
        // D级：东北

        // case when
        // 根据多个条件，不同的条件对应不同的值
        // case when then ... when then ... else ... end

        String sql =
                "SELECT "
                        + "area,"
                        + "CASE "
                        + "WHEN area='China North' OR area='China East' THEN 'A Level' "
                        + "WHEN area='China South' OR area='China Middle' THEN 'B Level' "
                        + "WHEN area='West North' OR area='West South' THEN 'C Level' "
                        + "ELSE 'D Level' "
                        + "END area_level,"
                        + "product_id,"
                        + "click_count,"
                        + "city_infos,"
                        + "product_name,"
                        + "product_status "
                        + "FROM ("
                        + "SELECT "
                        + "area,"
                        + "product_id,"
                        + "click_count,"
                        + "city_infos,"
                        + "product_name,"
                        + "product_status,"
                        + "row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
                        + "FROM tmp_area_fullprod_click_count "
                        + ") t "
                        + "WHERE rank<=3";

        DataFrame df = sqlContext.sql(sql);

        return df.javaRDD();
    }

    /**
     * 生成区域商品点击次数临时表（包含了商品的完整信息）
     * @param sqlContext
     */
    private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
        String sql =
                "SELECT "
                        + "tapcc.area,"
                        + "tapcc.product_id,"
                        + "tapcc.click_count,"
                        + "tapcc.city_infos,"
                        + "pi.product_name,"
                        + "if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status "
                        + "FROM tmp_area_product_click_count tapcc "
                        + "JOIN product_info pi ON tapcc.product_id=pi.product_id ";
        DataFrame df= sqlContext.sql(sql);
        df.registerTempTable("mp_area_fullprod_click_count");
    }

    /**
     * 生成各区域各商品点击次数临时表
     * @param sqlContext
     */
    private static void generateTempAreaPrdocutClickCountTable(SQLContext sqlContext) {

        // 按照area和product_id两个字段进行分组
        // 计算出各区域各商品的点击次数
        // 可以获取到每个area下的每个product_id的城市信息拼接起来的串
        String sql =
                "SELECT "
                        + "area,"
                        + "product_id,"
                        + "count(*) click_count, "
                        + "group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos "
                        + "FROM tmp_click_product_basic "
                        + "GROUP BY area,product_id ";
        // 使用Spark SQL执行这条SQL语句
        DataFrame df = sqlContext.sql(sql);

        df.registerTempTable("tmp_area_product_click_count");
    }


    /**
     * @param sqlContext
     * @param clickActionRDD
     * @param cityInfoRDD
     */
    private static void generateTempClickProductBasicTable(SQLContext sqlContext,
                                                           JavaPairRDD<Long, Row> clickActionRDD,
                                                           JavaPairRDD<Long, Row> cityInfoRDD) {
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = clickActionRDD.join(cityInfoRDD);
        // 将上面的JavaPairRDD，转换成一个JavaRDD<Row>（才能将RDD转换为DataFrame）
        JavaRDD<Row> mappedRDD = joinedRDD.map(line -> {
            long cityid = line._1;
            Row clickAction = line._2._1;
            Row cityInfo = line._2._2;

            long productid = clickAction.getLong(1);
            String cityName = cityInfo.getString(1);
            String area = cityInfo.getString(2);

            return RowFactory.create(cityid, cityName, area, productid);
        });

        // 基于JavaRDD<Row>的格式，就可以将其转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));

        StructType schema = DataTypes.createStructType(structFields);
        DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);
        // 将DataFrame中的数据，注册成临时表（tmp_clk_prod_basic）
        df.registerTempTable("tmp_clk_prod_basic");
    }

    /**
     * 使用Spark SQL从MySQL中查询城市信息
     *
     * @param sqlContext
     * @return
     */
    private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SQLContext sqlContext) {
        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://localhost:3306/spark_project");
        options.put("dbtable", "city_info");
        options.put("user", "root");
        options.put("password", "root");

        DataFrame cityInfoDF = sqlContext.read().format("jdbc")
                .options(options)
                .load();

        JavaRDD<Row> cityInfoRDD = cityInfoDF.toJavaRDD();

        return cityInfoRDD.mapToPair(line -> {
            long cityid = line.getLong(0);

            return new Tuple2<>(cityid, line);
        });

    }

    /**
     * 查询指定日期范围内的点击行为数据
     *
     * @param sqlContext
     * @param startDate
     * @param endDate
     * @return 点击行为数据
     */
    private static JavaPairRDD<Long, Row> getClickActionRDDByDate(SQLContext sqlContext,
                                                                  String startDate,
                                                                  String endDate) {
        // 从user_visit_action中，查询用户访问行为数据
        // 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
        // 第二个限定：在用户指定的日期范围内的数据

        String sql =
                "SELECT "
                        + "city_id,"
                        + "click_product_id product_id "
                        + "FROM user_visit_action "
                        + "WHERE click_product_id IS NOT NULL "
                        + "AND click_product_id != 'NULL' "
                        + "AND click_product_id != 'null' "
                        + "AND action_time>='" + startDate + "' "
                        + "AND action_time<='" + endDate + "'";

        DataFrame clickActionDF = sqlContext.sql(sql);
        return clickActionDF.toJavaRDD().mapToPair(line -> {
            long cityid = line.getLong(0);
            return new Tuple2<>(cityid, line);
        });

    }
}
