package ru.knastnt.kafkatest;

import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@EnableKafka
@Component
public class Listener {

    //Стандартный слушатель (класс настроек поставляется спрингбутом)

    //Стандартный слушатель
    /*
        група консьюмеров (это группа в рамках которой доставляется
        один экземпляр сообщения. Например, у Вас есть три консьюмера
        в одной группе, и все они слушают одну тему. Как только на
        сервере появляется новое сообщение с данной темой, оно
        доставляется кому-то одному из группы. Остальные два
        консьюмера сообщение не получают.)
    */
    @KafkaListener(topics = "msg", groupId = "myDefaultGroup")
    public void messageListener(String msg) {
        System.out.println(msg);
    }

    //################################################################################################################


    //Кастомные слушатели (класс настроек: ru\knastnt\kafkatest\KafkaConsumerConfig.java)

    //Кастомный Json слушатель для класса UserDTO
    @KafkaListener(topics = "msg2", containerFactory = "userKafkaListenerContainerFactory", groupId = "usersConsumers")
    public void messageListener(@Payload UserDTO userDTO, @Headers MessageHeaders headers) {
        System.out.println(headers);
        System.out.println(userDTO);
    }

    //Кастомный Json слушатель для класса Address
    @KafkaListener(topics = "msg3", containerFactory = "addresKafkaListenerContainerFactory", groupId = "addressesConsumers")
    public void messageListener(@Payload UserDTO.Address s, @Headers MessageHeaders headers) {
        System.out.println(headers);
        System.out.println(s);
    }
}
