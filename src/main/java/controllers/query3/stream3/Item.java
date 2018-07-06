package controllers.query3.stream3;

import java.util.ArrayList;

public class Item {


    public Item() {
        this.slidingWindow = new ArrayList<>();
    }
    private Long id;
    private String name;
    ArrayList<Integer> slidingWindow;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ArrayList<Integer> getSlidingWindow() {
        return slidingWindow;
    }

    public void setSlidingWindow(ArrayList<Integer> slidingWindow) {
        this.slidingWindow = slidingWindow;
    }

    public Integer returnFirstItem(){
        return this.slidingWindow.get(0);
    }
}
