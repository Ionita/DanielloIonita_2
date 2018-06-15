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
    private Integer month;
    private Integer year;


    public Date getTmp() {
        return tmp;
    }

    public void setTmp(Date tmp) {

        this.tmp = tmp;
        Calendar c = GregorianCalendar.getInstance();
        c.setTime(tmp);
        setHour(c.get(Calendar.HOUR_OF_DAY));
        setDay(c.get(Calendar.DAY_OF_MONTH));
        setMonth(c.get(Calendar.MONTH));
        setYear(c.get(Calendar.YEAR));

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

    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(Integer month) {
        this.month = month;
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
}
