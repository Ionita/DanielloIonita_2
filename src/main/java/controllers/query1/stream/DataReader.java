package controllers.query1.stream;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import entities.Friend;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DataReader {

    private final static String dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

    public  DataReader() {}

    public static ArrayList<Friend> getData(String jsonString){

        ArrayList<Friend> messages = new ArrayList<>();
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(jsonString);

        if (element.isJsonObject()) {
            try {
                JsonObject jsonRecord = element.getAsJsonObject();
                Friend data = new Friend();
                try {
                    data.setTmp(new SimpleDateFormat(dateFormat).parse(jsonRecord.get("tmp").getAsString()));
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                //data.setTmp(new Date(Long.parseLong(jsonRecord.get("tmp").getAsString())));

                data.setUser_1(jsonRecord.get("user_id1").getAsLong());
                data.setUser_2(jsonRecord.get("user_id2").getAsLong());

                messages.add(data);
            } catch (Exception e){
                System.out.println("Eccezione");
            }

        } else
            System.out.println("Not a valid Json Element");

        return messages;
    }
}