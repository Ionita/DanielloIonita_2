package controllers.query3.stream;

import java.util.ArrayList;

public class Query3_Item {
    private Long tmp;
    private Long user_id;
    private ArrayList<Integer> slidingWindow;
    private Integer dailyValue = 0;
    private Integer weekValue = 0;

    public Query3_Item(int windowSize) {
        slidingWindow = new ArrayList<>();
        for (int i = 0; i< windowSize +1; i++){ //rivedi il + 1
            slidingWindow.add(0);
        }
    }

    public Long getTmp() {
        return tmp;
    }

    public void setTmp(String tmp) {
        this.tmp = Long.parseLong(tmp);
    }

    public ArrayList<Integer> getSlidingWindow() {
        return slidingWindow;
    }

    public Integer getFirstWindowPosition(){
        return getSlidingWindow().get(0);
    }

    public Integer getDailyValue() {
        return dailyValue;
    }

    public void setDailyValue(Integer dailyValue) {
        this.dailyValue = dailyValue;
    }

    public Integer getWeekValue() {
        return weekValue;
    }

    public void setWeekValue(Integer weekValue) {
        this.weekValue = weekValue;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }
}
