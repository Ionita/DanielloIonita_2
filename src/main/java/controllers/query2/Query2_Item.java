package controllers.query2;

import java.util.ArrayList;

public class Query2_Item {
    private Long tmp;
    private Long post_id;
    private ArrayList<Integer> slidingWindow;

    public Query2_Item (int windowSize) {
        slidingWindow = new ArrayList<>();
        for (int i = 0; i< windowSize; i++){
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

}
