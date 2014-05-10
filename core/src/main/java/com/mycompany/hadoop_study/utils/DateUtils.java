package com.mycompany.hadoop_study.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class DateUtils {

    public static Date incrDate(Date date) {

        Calendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_YEAR, 1);
        return cal.getTime();

    }

    public static Date incrDate(Date date, int days) {

        Calendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_YEAR, days);
        return cal.getTime();

    }

    public static Date incrMonth(Date date, int months) {
        Calendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.MONTH, months);
        return cal.getTime();
    }

    /**
     * Converts date to YYYYMMDD format
     *
     * @param date
     * @return
     */
    public static String dateToYYYYMMDD(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        return dateFormat.format(date);
    }

    /**
     * Converts date to YYYYMMDD format
     *
     * @param date
     * @return
     */
    // TODO , add this for event collector
    /*public static String dateToYYYYMMDDHHMM(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmm");
        return dateFormat.format(date);
    }*/

    /**
     * Converts a string in the format YYYYMMDD to date
     *
     * @param dt
     * @return
     */
    public static Date YYYYMMDDToDate(String dt) {
        try {
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            return df.parse(dt);
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
    //2009-09-28 04:00:00 to Date
    public static Date YYYYMMDDHHMMSSToDate(String dt) {
        try {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return df.parse(dt);
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static long dateDiffInDays(Date startDate, Date endDate) {
        Calendar sCal = new GregorianCalendar();
        sCal.setTime(startDate);
        Calendar eCal = new GregorianCalendar();
        eCal.setTime(endDate);
        return (eCal.getTimeInMillis() - sCal.getTimeInMillis()) / (24 * 60 * 60 * 1000);
    }

    public static String dateDiffInWeeks(Date startDate, Date endDate) {
        Long ioDays = dateDiffInDays(startDate, endDate);
        String ioWeeks = ((int) (ioDays/7)) + "w";
        ioWeeks += ((ioDays%7) == 0) ? "" : " " + (ioDays%7) + "d";
        return ioWeeks;
    }
}
