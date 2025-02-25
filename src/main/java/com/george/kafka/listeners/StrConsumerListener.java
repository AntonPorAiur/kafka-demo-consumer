package com.george.kafka.listeners;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Component
public class StrConsumerListener {

    private static Logger log = LoggerFactory.getLogger(StrConsumerListener.class);

    // Offset es un identificador, del lugar que tiene el mensaje
    @KafkaListener(groupId = "group-1",
            topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"0"}),
            containerFactory = "strContainerFactory")
    public void listener1(String message){
        log.info("LISTENER1 ::: Recibiendo un mensaje{ }", message);
    }

    @KafkaListener(groupId = "group-1",
            topicPartitions = @TopicPartition(topic = "str-topic", partitions = {"1"}),
            containerFactory = "strContainerFactory")
    public void listener2(String message){
        log.info("LISTENER2 ::: Recibiendo un mensaje{ }", message);
    }

    // Este escucha a todas las particiones
    @KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "strContainerFactory")
    public void listener3(String message){
        log.info("LISTENER3 ::: Recibiendo un mensaje{ }", message);
    }

}
