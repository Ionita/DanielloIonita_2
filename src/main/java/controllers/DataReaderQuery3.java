package controllers;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import entities.TopUsers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class DataReaderQuery3 {

    private final static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    public  DataReaderQuery3() {}

    public static ArrayList<TopUsers> getData(String jsonString){

        ArrayList<TopUsers> topUsers = new ArrayList<>();
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(jsonString);

        if (element.isJsonObject()) {
            JsonObject jsonRecord = element.getAsJsonObject();
            if (jsonRecord.get("type").getAsInt() == 0)
                isFriendship(jsonRecord, topUsers);
            else if (jsonRecord.get("type").getAsInt() == 1)
                isPost(jsonRecord, topUsers);
            else
                isCommented(jsonRecord, topUsers);
        }

        return topUsers;
    }

    private static void isFriendship (JsonObject jsonString, ArrayList<TopUsers> topUsers) {

        TopUsers a = new TopUsers();
        TopUsers b = new TopUsers();

        try {
            a.setTmp(new SimpleDateFormat(dateFormat).parse(jsonString.get("tmp").getAsString()));
            b.setTmp(new SimpleDateFormat(dateFormat).parse(jsonString.get("tmp").getAsString()));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        a.setUser_id(jsonString.get("user_id1").getAsLong());
        b.setUser_id(jsonString.get("user_id2").getAsLong());
        topUsers.add(a);
        topUsers.add(b);
    }

    private static void isPost (JsonObject jsonString, ArrayList<TopUsers> topUsers) {

        TopUsers a = new TopUsers();

        try {
            a.setTmp(new SimpleDateFormat(dateFormat).parse(jsonString.get("tmp").getAsString()));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        a.setUser_id(jsonString.get("user_id1").getAsLong());
        topUsers.add(a);

    }

    private static void isCommented (JsonObject jsonString, ArrayList<TopUsers> topUsers) {

        TopUsers a = new TopUsers();

        try {
            a.setTmp(new SimpleDateFormat(dateFormat).parse(jsonString.get("tmp").getAsString()));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        a.setUser_id(jsonString.get("user_id1").getAsLong());
        topUsers.add(a);

    }

}