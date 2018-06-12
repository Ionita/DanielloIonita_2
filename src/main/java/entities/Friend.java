package entities;

import java.sql.Timestamp;

public class Friend {
    private Timestamp tmp;
    private Integer user_1;
    private Integer user_2;

    public Timestamp getTmp() {
        return tmp;
    }

    public void setTmp(Timestamp tmp) {
        this.tmp = tmp;
    }

    public Integer getUser_1() {
        return user_1;
    }

    public void setUser_1(Integer user_1) {
        this.user_1 = user_1;
    }

    public Integer getUser_2() {
        return user_2;
    }

    public void setUser_2(Integer user_2) {
        this.user_2 = user_2;
    }
}
