import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ShoppingProducer {
    public static void main(String[] args) {

        KafkaProducer producer;
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer(props);
        Scanner input = new Scanner(System.in);
        Scanner myObj = new Scanner(System.in);
        System.out.println("Enter Model:");
        String model = myObj.nextLine();
        System.out.println("Enter Manufacturing Date:");
        String manu_Date = myObj.nextLine();
        System.out.println("Enter Release Year:");
        String release_Year = myObj.nextLine();
        System.out.println("Enter colour:");
        String color = myObj.nextLine();
        System.out.println("Enter Price:");
        String price = myObj.nextLine();
        String sendValue = String.format("{'model':" + model + ",'manufacture_date':"+manu_Date+",'release_year':" + release_Year + " ,'color':"+color+",'price':" + price + ",'color':" + color + "}");
        producer.send(new ProducerRecord("temp", sendValue));
    }
}
