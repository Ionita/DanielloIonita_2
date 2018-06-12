package entities;

import java.sql.Timestamp;

public class Comment {
    private Timestamp tmp;
    private Integer comment_id;
    private Integer user_id;
    private String comment;
    private String user_name;
    private Integer comment_replied;
    private Integer post_commented;

    public Timestamp getTmp() {
        return tmp;
    }

    public void setTmp(Timestamp tmp) {
        this.tmp = tmp;
    }

    public Integer getComment_id() {
        return comment_id;
    }

    public void setComment_id(Integer comment_id) {
        this.comment_id = comment_id;
    }

    public Integer getUser_id() {
        return user_id;
    }

    public void setUser_id(Integer user_id) {
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

    public Integer getComment_replied() {
        return comment_replied;
    }

    public void setComment_replied(Integer comment_replied) {
        this.comment_replied = comment_replied;
    }

    public Integer getPost_commented() {
        return post_commented;
    }

    public void setPost_commented(Integer post_commented) {
        this.post_commented = post_commented;
    }
}
