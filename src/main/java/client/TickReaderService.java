package client;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.stream.JsonReader;


public class TickReaderService {
    public void getTickStream(TickStreamCallback callback, InputStream in) {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(LocalDateTime.class,
                        (JsonDeserializer<LocalDateTime>) (json, typeOfT, context) ->
                                json == null ? null : LocalDateTime.parse(json.getAsString(), ISO_OFFSET_DATE_TIME)
                ).create();

        try (JsonReader reader=new JsonReader(new InputStreamReader(in,"UTF-8"))){
            reader.beginArray();
            while (reader.hasNext()) {
                Tick tick = gson.fromJson(reader, Tick.class);
                callback.handle(tick);
            }
            reader.endArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
