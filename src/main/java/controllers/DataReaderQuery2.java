package controllers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import entities.Comment;
import entities.Friend;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class DataReaderQuery2 {

    private final static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    public  DataReaderQuery2() {}

    public static ArrayList<Comment> getData(String jsonString){

        ArrayList<Comment> comments = new ArrayList<>();
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(jsonString);

        if (element.isJsonObject()) {
            try {
                JsonObject jsonRecord = element.getAsJsonObject();
                Comment data = new Comment();
                try {
                    data.setTmp(new SimpleDateFormat(dateFormat).parse(jsonRecord.get("tmp").getAsString()));
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                data.setComment_id(jsonRecord.get("comment_id").getAsLong());
                data.setUser_id(jsonRecord.get("user_id").getAsLong());
                data.setComment(jsonRecord.get("comment").getAsString());
                data.setUser_name(jsonRecord.get("user_name").getAsString());
                if (jsonRecord.get("comment_replied").isJsonNull() && !jsonRecord.get("post_commented").isJsonNull()) {

                    data.setComment_replied(null);
                    data.setPost_commented(jsonRecord.get("post_commented").getAsLong());
                }
                else {
                    data.setComment_replied(jsonRecord.get("comment_replied").getAsLong());
                    data.setPost_commented(null);
                }

                comments.add(data);
            } catch (Exception e){
                System.out.println("Eccezione");
            }

        } else
            System.out.println("Not a valid Json Element");

        return comments;
    }
}