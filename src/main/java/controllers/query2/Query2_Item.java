package controllers.query2;

import java.util.ArrayList;

public class Query2_Item {
    private Long tmp;
    private Long post_id;
    private ArrayList<Integer> slidingWindow;

    private Query2_Item (Integer windowSize) {
        slidingWindow = new ArrayList<>();
    }

    public Long getTmp() {
        return tmp;
    }

    public void setTmp(Long tmp) {
        this.tmp = tmp;
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

    public void setSlidingWindow(ArrayList<Integer> slidingWindow) {
        this.slidingWindow = slidingWindow;
    }
}
