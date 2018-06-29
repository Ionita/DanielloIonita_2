package entities;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class Comment {
    private Date tmp;
    private Long comment_id;
    private Long user_id;
    private String comment;
    private String user_name;
    private Long comment_replied;
    private Long post_commented;

    private Integer hour;

    public Date getTmp() {
        return tmp;
    }

    public void setTmp(Date tmp) {
        this.tmp = tmp;
        Calendar c = GregorianCalendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"));
        c.setTime(tmp);
        setHour(c.get(Calendar.HOUR_OF_DAY));    }

    public Long getComment_id() {
        return comment_id;
    }

    public void setComment_id(Long comment_id) {
        this.comment_id = comment_id;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public Long getComment_replied() {
        return comment_replied;
    }

    public void setComment_replied(Long comment_replied) {
        this.comment_replied = comment_replied;
    }

    public Long getPost_commented() {
        return post_commented;
    }

    public void setPost_commented(Long post_commented) {
        this.post_commented = post_commented;
    }

    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }
}
