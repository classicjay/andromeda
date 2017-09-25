package com.bonc.dw3.common.util;

import com.bonc.dw3.service.SystemVariableService;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


/**
 * 日期工具类
 * @author Administrator
 *
 */
public class DateUtils{
	    /**  
	     * 计算两个日期之间相差的天数  
	     * @param smdate 较小的时间 
	     * @param bdate  较大的时间 
	     * @return 相差天数 
	     * @throws ParseException  
	     */    
	    public static int daysBetween(Date smdate,Date bdate) throws ParseException    
	    {    
	        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");  
	        smdate=sdf.parse(sdf.format(smdate));  
	        bdate=sdf.parse(sdf.format(bdate));  
	        Calendar cal = Calendar.getInstance();    
	        cal.setTime(smdate);    
	        long time1 = cal.getTimeInMillis();                 
	        cal.setTime(bdate);    
	        long time2 = cal.getTimeInMillis();         
	        long between_days=(time2-time1)/(1000*3600*24);  
	            
	       return Integer.parseInt(String.valueOf(between_days));           
	    } 

	    
	    /**
	     * 获取相对账期
	     * @param date
	     * @return
	     * @throws ParseException 
	     */
	    public static String getDate(String date,String count,String dateType) throws ParseException {
			String dateStr = date.replaceAll("-","");
			SimpleDateFormat sdf = null; 
			if(dateType.equals(SystemVariableService.acctTypeDay)){
				sdf = new SimpleDateFormat("yyyyMMdd");
			}else{
				sdf = new SimpleDateFormat("yyyyMM");
			}
			Date d1 = sdf.parse(dateStr);
	    	int a = Integer.parseInt(count);
	        Calendar cal = Calendar.getInstance();
	        cal.setTime(d1);
	        if(dateType.equals(SystemVariableService.acctTypeDay)){
	        	cal.add(Calendar.DATE, a);
	        }else{
	        	cal.add(Calendar.MONTH, a);
	        }
	        Date d2 = cal.getTime();
	        String res = sdf.format(d2).replace("-", "");
	        return res;
	    }  
	    
	    /**
	     * 构造连续7天账期 
	     * @param date
	     * @param dateType
	     * @param days
	     * @return
	     * @throws ParseException
	     */
	    
	    public static List<String> dateProcess(String date,String dateType,int days) throws ParseException{
			List<String> dateList = new ArrayList<>();
	    	SimpleDateFormat sdf = null;
			if(dateType.equals(SystemVariableService.acctTypeDay)){
				 sdf=new SimpleDateFormat("yyyyMMdd");//小写的mm表示的是分钟  
			}else{
				sdf=new SimpleDateFormat("yyyyMM");//小写的mm表示的是分钟  
			}
	        for(int i=days;i>0;i--){
	    		String str = DateUtils.getDate(date, "1",dateType);
	    		Date d1 = sdf.parse(str);
	            Calendar cal = Calendar.getInstance();
	            cal.setTime(d1);
	        	if(dateType.equals(SystemVariableService.acctTypeDay)){
	        		cal.add(Calendar.DATE, -i);
	        	}else{
	        		cal.add(Calendar.MONTH, -i);
	        	}          
	          Date da = cal.getTime();
	          String dateStr = sdf.format(da);
	          dateList.add(dateStr);
	       }
	    	return dateList;
	    }

	/**
	 * 构造两个时间的连续账期
	 * @param startDate 开始时间
	 * @param endDate 结束时间
	 * @param dateType 日月标识
	 * @return
	 * @throws ParseException
	 */
	public static List<String> dateBetweeWith(String startDate, String endDate, String dateType){
		List<String> dateList = new ArrayList<>();
		SimpleDateFormat sdf = null;
		if(dateType.equals(SystemVariableService.acctTypeDay)){
			sdf=new SimpleDateFormat("yyyyMMdd");//小写的mm表示的是分钟
		}else{
			startDate = CommonUtils.isNotBlank(startDate) && startDate.length() > 6 ? startDate.substring(0, 6):startDate;
			endDate = CommonUtils.isNotBlank(endDate) && endDate.length() > 6 ? endDate.substring(0, 6):endDate;
			sdf=new SimpleDateFormat("yyyyMM");//小写的mm表示的是分钟
		}
		Date dStart = null;
		Date dEnd = null;
		try {
			dStart = sdf.parse(startDate);
			dEnd = sdf.parse(endDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar calStart = Calendar.getInstance();
		calStart.setTime(dStart);
		Calendar calEnd = Calendar.getInstance();
		calEnd.setTime(dEnd);
		dateList.add(sdf.format(dStart));
		if(dateType.equals(SystemVariableService.acctTypeDay)){
			long day=Math.abs((dEnd.getTime()-dStart.getTime())/(24*60*60*1000));
			for(int i=0; i< day; i++){
				calStart.add(Calendar.DATE, 1);
				dateList.add(sdf.format(calStart.getTime()));
			}
		}else{
			int yearNumber = calEnd.get(calEnd.YEAR) - calStart.get(calEnd.YEAR);
			int monthNum = calEnd.get(Calendar.MONTH) - calStart.get(Calendar.MONTH);
			int day = yearNumber*12 + monthNum;
			for(int i=0; i< day; i++){
				calStart.add(Calendar.MONTH, 1);
				dateList.add(sdf.format(calStart.getTime()));
			}
		}
		return dateList;
	}
}
