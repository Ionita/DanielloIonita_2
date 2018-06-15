package entities;

import java.sql.Timestamp;

public class Friend {
    private Timestamp tmp;
    private Long user_1;
    private Long user_2;


    public Timestamp getTmp() {
        return tmp;
    }

    public void setTmp(Timestamp tmp) {
        this.tmp = tmp;
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
}
