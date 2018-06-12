package entities;

import java.io.Serializable;
import java.sql.Timestamp;

public class Message implements Serializable {

    private Integer type;
    private Timestamp tmp;
    private Integer user_id;
    private String user_name;
    private Integer comment_id;
    private String comment;
    private Integer post_id;
    private String post;
    private Integer user_id2;


    public Message (Integer type){
        this.type = type;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public Timestamp getTmp() {
        return tmp;
    }

    public void setTmp(Timestamp tmp) {
        this.tmp = tmp;
    }

    public Integer getUser_id() {
        return user_id;
    }

    public void setUser_id(Integer user_id) {
        this.user_id = user_id;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }

    public Integer getComment_id() {
        return comment_id;
    }

    public void setComment_id(Integer comment_id) {
        this.comment_id = comment_id;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public Integer getPost_id() {
        return post_id;
    }

    public void setPost_id(Integer post_id) {
        this.post_id = post_id;
    }

    public String getPost() {
        return post;
    }

    public void setPost(String post) {
        this.post = post;
    }

    public Integer getUser_id2() {
        return user_id2;
    }

    public void setUser_id2(Integer user_id2) {
        this.user_id2 = user_id2;
    }

}
