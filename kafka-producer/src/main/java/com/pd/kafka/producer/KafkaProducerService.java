package com.pd.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@Component
public class KafkaProducerService {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void sendMessage(String topic, Object data) {

        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, data);

        future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {

            @Override
            public void onSuccess(@Nullable SendResult<String, Object> stringObjectSendResult) {
                log.info("发送消息成功： " + stringObjectSendResult.toString());
            }

            @Override
            public void onFailure(Throwable throwable) {
                log.error("发送消息失败：" + throwable.getMessage());
            }
        });
    }

}
