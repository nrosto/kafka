package ru.knastnt.kafkatest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.text.SimpleDateFormat;
import java.util.Date;

import static java.lang.Thread.sleep;


@SpringBootApplication
public class KafkatestApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkatestApplication.class, args);
    }

    //it's ok. https://stackoverflow.com/questions/55280173/the-correct-way-for-creation-of-kafkatemplate-in-spring-boot
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")

    @Component
    public static class Runner implements CommandLineRunner{

        //Стандартный шаблон
        @Autowired
        private KafkaTemplate<String, Object> kafkaTemplate;

        //Кастомные шаблоны
        @Autowired
        private KafkaTemplate<String, UserDTO> kafkaUserTemplate;
        @Autowired
        private KafkaTemplate<String, UserDTO.Address> kafkaAddressTemplate;


        @Override
        public void run(String... args) throws Exception {
            Thread thread = new Thread(() -> {
                try {
                    while (true) {
                        //Шлём строку
                        sleep(2000);
                        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send("msg", "currentTime", "сообщение: " + (new SimpleDateFormat("dd MM yyyy HH-mm-ss")).format(new Date()));
                        future.addCallback(System.out::println, System.err::println);

                        //Шлём массив
                        sleep(2000);
                        future = kafkaTemplate.send("msg", "array", new int[]{1,3,5,7,9,0});
                        future.addCallback(System.out::println, System.err::println);


                        //Шлём объекты через кастомные шаблоны
                        sleep(2000);
                        ListenableFuture<SendResult<String, UserDTO>> userFuture = kafkaUserTemplate.send("msg2", "user", UserDTO.getTestInstance());
                        userFuture.addCallback(System.out::println, System.err::println);
                        sleep(2000);
                        ListenableFuture<SendResult<String, UserDTO.Address>> userFuture2 = kafkaAddressTemplate.send("msg3", "addr", UserDTO.getTestInstance().getAddress());
                        userFuture2.addCallback(System.out::println, System.err::println);
                    }
                }catch (InterruptedException e){}
            });
            thread.setDaemon(true);
            thread.start();
        }

    }
}
