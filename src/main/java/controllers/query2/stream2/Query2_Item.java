package controllers.query2.stream2;

import java.util.ArrayList;

public class Query2_Item {
    private Long tmp;
    private Long post_id;
    private ArrayList<Integer> slidingWindow;
    private Integer dailyValue = 0;
    private Integer weekValue = 0;

    public Query2_Item(int windowSize) {
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

    public Long getPost_id() {
        return post_id;
    }

    public void setPost_id(Long post_id) {
        this.post_id = post_id;
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
}
