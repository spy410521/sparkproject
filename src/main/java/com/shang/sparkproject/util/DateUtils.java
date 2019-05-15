package com.shang.sparkproject.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期时间工具类
 * @author Administrator
 *
 */
public class DateUtils {

//	public static final SimpleDateFormat TIME_FORMAT =
//			new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//	public static final SimpleDateFormat DATE_FORMAT =
//			new SimpleDateFormat("yyyy-MM-dd");
//	public static final SimpleDateFormat DATEKEY_FORMAT =
//			new SimpleDateFormat("yyyyMMdd");

	//使用ThreadLocal将SimpleDateFormat对象变成线程安全的，每个线程中都一个SimpleDateFormat属于该线程的局部对象，
	//各个线程中的SimpleDateFormat对象互不干扰，消除多个线程同时访问SimpleDateFormat的线程不安全
	//可以使用synchronized，不推荐使用，
	private static final ThreadLocal<SimpleDateFormat> messageFormat=new ThreadLocal<SimpleDateFormat>();

	private static final SimpleDateFormat getTimeFormat(){
		SimpleDateFormat  timeFormat=messageFormat.get();
		if(timeFormat==null){
			 timeFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
		return timeFormat;
	}

	private static final SimpleDateFormat getDateFormat(){
		SimpleDateFormat dateFormat=messageFormat.get();
		if(dateFormat==null){
			dateFormat=new SimpleDateFormat("yyyy-MM-dd");
		}
		return dateFormat;
	}

	private static final SimpleDateFormat getDateKeyFormat(){
		SimpleDateFormat dateKeyFormat=messageFormat.get();
		if(dateKeyFormat==null){
			dateKeyFormat=new SimpleDateFormat("yyyyMMdd");
		}
		return dateKeyFormat;
	}



	/**
	 * 判断一个时间是否在另一个时间之前
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 判断结果
	 */
	public static boolean before(String time1, String time2) {
		try {
			Date dateTime1 = getTimeFormat().parse(time1);
			Date dateTime2 = getTimeFormat().parse(time2);
			
			if(dateTime1.before(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 判断一个时间是否在另一个时间之后
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 判断结果
	 */
	public static boolean after(String time1, String time2) {
		try {
			Date dateTime1 = getTimeFormat().parse(time1);
			Date dateTime2 = getTimeFormat().parse(time2);
			
			if(dateTime1.after(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 计算时间差值（单位为秒）
	 * @param time1 时间1
	 * @param time2 时间2
	 * @return 差值
	 */
	public static int minus(String time1, String time2) {
		try {
			Date datetime1 = getTimeFormat().parse(time1);
			Date datetime2 = getTimeFormat().parse(time2);
			
			long millisecond = datetime1.getTime() - datetime2.getTime();
			
			return Integer.valueOf(String.valueOf(millisecond / 1000));  
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取年月日和小时
	 * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
	 * @return 结果（yyyy-MM-dd_HH）
	 */
	public static String getDateHour(String datetime) {
		String date = datetime.split(" ")[0];
		String hourMinuteSecond = datetime.split(" ")[1];
		String hour = hourMinuteSecond.split(":")[0];
		return date + "_" + hour;
	}  
	
	/**
	 * 获取当天日期（yyyy-MM-dd）
	 * @return 当天日期
	 */
	public static String getTodayDate() {
		return getDateFormat().format(new Date());
	}
	
	/**
	 * 获取昨天的日期（yyyy-MM-dd）
	 * @return 昨天的日期
	 */
	public static String getYesterdayDate() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());  
		cal.add(Calendar.DAY_OF_YEAR, -1);  
		
		Date date = cal.getTime();
		
		return getDateFormat().format(date);
	}
	
	/**
	 * 格式化日期（yyyy-MM-dd）
	 * @param date Date对象
	 * @return 格式化后的日期
	 */
	public static String formatDate(Date date) {
		return getDateFormat().format(date);
	}
	
	/**
	 * 格式化时间（yyyy-MM-dd HH:mm:ss）
	 * @param date Date对象
	 * @return 格式化后的时间
	 */
	public static String formatTime(Date date) {
		if(null!=date){
			return getTimeFormat().format(date);
		}
		return null;
	}
	
	/**
	 * 解析时间字符串
	 * @param time 时间字符串 
	 * @return Date
	 */
	public static Date parseTime(String time) {
		try {
			return getTimeFormat().parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 格式化日期key
	 * @param date
	 * @return
	 */
	public static String formatDateKey(Date date) {
		return getDateKeyFormat().format(date);
	}
	
	/**
	 * 格式化日期key
	 * @param datekey
	 * @return
	 */
	public static Date parseDateKey(String datekey) {
		try {
			return getDateKeyFormat().parse(datekey);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 格式化时间，保留到分钟级别
	 * yyyyMMddHHmm
	 * @param date
	 * @return
	 */
	public static String formatTimeMinute(Date date) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");  
		return sdf.format(date);
	}
	
}
