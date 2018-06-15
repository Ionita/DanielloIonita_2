package entities;

import java.io.Serializable;

public class Message implements Serializable {

    private Integer type;
    private String tmp;
    private Long user_id1;
    private String user_name;
    private Long comment_id;
    private String comment;
    private Long post_id;
    private String post;
    private Long user_id2;
    private Long comment_replied;
    private Long post_commented;

    public Message(){}

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

    public Message (Integer type){
        this.type = type;
    }


    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }


    public String getTmp() {
        return tmp;
    }

    public void setTmp(String tmp) {
        this.tmp = tmp;
    }


    public Long getUser_id1() {
        return user_id1;
    }

    public void setUser_id1(Long user_id) {
        this.user_id1 = user_id;
    }


    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }


    public Long getComment_id() {
        return comment_id;
    }

    public void setComment_id(Long comment_id) {
        this.comment_id = comment_id;
    }


    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }


    public Long getPost_id() {
        return post_id;
    }

    public void setPost_id(Long post_id) {
        this.post_id = post_id;
    }


    public String getPost() {
        return post;
    }

    public void setPost(String post) {
        this.post = post;
    }


    public Long getUser_id2() {
        return user_id2;
    }

    public void setUser_id2(Long user_id2) {
        this.user_id2 = user_id2;
    }

}
