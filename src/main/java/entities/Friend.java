package entities;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class Friend {
    private Date tmp;
    private Long user_1;
    private Long user_2;

    private Integer hour;
    private Integer day;
    private Integer year;
    private Integer week;


    public Date getTmp() {
        return tmp;
    }

    public void setTmp(Date tmp) {

        this.tmp = tmp;
        Calendar c = GregorianCalendar.getInstance();
        c.setTime(tmp);
        setDay(c.get(Calendar.DAY_OF_YEAR));
        setYear(c.get(Calendar.YEAR));
        setWeek(c.get(Calendar.WEEK_OF_YEAR));
        setHour(c.get(Calendar.HOUR_OF_DAY));


    }


    public Long getUser_1() {
        return user_1;
    }

    public void setUser_1(Long user_1) {
        this.user_1 = user_1;
    }


    public Long getUser_2() {
        return user_2;
    }

    public void setUser_2(Long user_2) {
        this.user_2 = user_2;
    }


    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }


    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }


    public Integer getWeek() {
        return week;
    }

    public void setWeek(Integer week) {
        this.week = week;
    }

    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }
}
