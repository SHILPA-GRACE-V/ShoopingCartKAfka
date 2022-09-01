import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Properties;
import java.sql.*;
public class consumer {
    public static void main(String[] args) {
        KafkaConsumer consumer;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("group.id", "test");
        consumer = new KafkaConsumer<>(props);
        KafkaConsumer obj = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("temp"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());

                String fetchedValue = record.value();
                JSONObject jsonObject = new JSONObject(fetchedValue);
                String model = String.valueOf(jsonObject.getString("model"));
                System.out.println(model);
                String manu_Date = String.valueOf(jsonObject.getString("manufacture_date"));
                System.out.println(manu_Date);
                String release_Year=String.valueOf(jsonObject.getInt("release_year"));
                System.out.println(release_Year);
                String color=String.valueOf(jsonObject.getString("color"));
                System.out.println(color);
                String price = String.valueOf(jsonObject.getInt("price"));
                System.out.println(price);


                Connection conn = null;
                Statement stmt = null;
                try {
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    conn = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/mobile", "root", "Sgv3#321");
                    System.out.println("connection successful");
                    stmt = (Statement) conn.createStatement();
                    String qry="insert into mobiles(model,manufacture_date,release_year,color,price) values('"+model+"','"+manu_Date+"',"+release_Year+",'"+color+"',"+price+")";
                    System.out.println(qry);
                    stmt.executeUpdate(qry);
                    System.out.println("Successful");
                } catch (Exception ex) {
                    System.out.println(ex);

                }
            }
        }
    }
}
