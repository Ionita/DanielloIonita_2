package entities;

import java.sql.Timestamp;

public class Post {
    private Timestamp tmp;
    private Long post_id;
    private Long user_id;
    private String post;
    private String user_name;

    public Timestamp getTmp() {
        return tmp;
    }

    public void setTmp(Timestamp tmp) {
        this.tmp = tmp;
    }

    public Long getPost_id() {
        return post_id;
    }

    public void setPost_id(Long post_id) {
        this.post_id = post_id;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public String getPost() {
        return post;
    }

    public void setPost(String post) {
        this.post = post;
    }

    public String getUser_name() {
        return user_name;
    }

    public void setUser_name(String user_name) {
        this.user_name = user_name;
    }
}
