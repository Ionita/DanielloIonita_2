package controllers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import entities.Friend;
import entities.Message;

import java.util.ArrayList;

public class DataReader {


    public  DataReader() {}

    public static ArrayList<Friend> getData(String jsonString){


        ArrayList<Friend> messages = new ArrayList<>();
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(jsonString);
        if (element.isJsonObject()) {
            JsonObject jsonRecord = element.getAsJsonObject();
            Friend data = new Friend();
            data.setUser_1(jsonRecord.get("user_id1").getAsLong());
            data.setUser_2(jsonRecord.get("user_id2").getAsLong());

            messages.add(data);
        } else {
            System.out.println("Not a valid Json Element");
        }


        return messages;
    }
}