package entities;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TopUsers {
    private Integer friends_number;
    private Integer posts_number;
    private Integer comments_number;

    //query 3
    private String username;
    private Long user_id;
    private Date tmp;
    private Integer hour;

    public Integer getFriends_number() {
        return friends_number;
    }

    public void setFriends_number(Integer friends_number) {
        this.friends_number = friends_number;
    }

    public Integer getPosts_number() {
        return posts_number;
    }

    public void setPosts_number(Integer posts_number) {
        this.posts_number = posts_number;
    }

    public Integer getComments_number() {
        return comments_number;
    }

    public void setComments_number(Integer comments_number) {
        this.comments_number = comments_number;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public Date getTmp() {
        return tmp;
    }

    public void setTmp(Date tmp) {
        this.tmp = tmp;
        Calendar c = GregorianCalendar.getInstance();
        c.setTime(tmp);
        setHour(c.get(Calendar.HOUR_OF_DAY));
    }

    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
