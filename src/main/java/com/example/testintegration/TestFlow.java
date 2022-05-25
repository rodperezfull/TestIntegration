package com.example.testintegration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.http.HttpMethod;
import org.springframework.integration.aggregator.SimpleMessageGroupProcessor;
import org.springframework.integration.annotation.Aggregator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.ScatterGatherSpec;
import org.springframework.integration.http.dsl.Http;
import org.springframework.integration.http.inbound.HttpRequestHandlingMessagingGateway;
import org.springframework.integration.store.MessageGroupFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import java.util.stream.Collectors;

@Configuration
@Slf4j
public class TestFlow {

  final TaskExecutor taskExecutor;

  public TestFlow(TaskExecutor threadPoolTaskExecutor) {
    this.taskExecutor = threadPoolTaskExecutor;
  }

  @Bean
  public HttpRequestHandlingMessagingGateway messageSourceS() {
    return Http.inboundGateway("/inputMessage")
            .requestMapping(r -> r.methods(HttpMethod.POST))
            .id("inputMessage")
            .get();

  }

  @Bean
  public DirectChannel sampleChannel() {
    return new DirectChannel();
  }

  @Bean
  public MessageChannel executorChannel(){
    return MessageChannels.executor("executorChannel", taskExecutor).get();
  }

  @Bean
  public MessageChannel executorChannel2(){
    return MessageChannels.executor("executorChannel2", taskExecutor).get();
  }

  @Bean
  public IntegrationFlow process1(){
    return IntegrationFlows.from(executorChannel())
            .log()
            .delay("delayer.messageGroupId", d -> d
                    .defaultDelay(3000L))
            .<String, String>transform(x -> x + "1")
            .get();
  }

  @Bean
  public IntegrationFlow process2(){
    log.info("process2");

    return IntegrationFlows.from(executorChannel2())
            .log()
            .delay("delayer.messageGroupId", d -> d
                    .defaultDelay(1000L))
            .<String, String>transform(x -> x + "2")
            .get();
  }



  @Bean
  public IntegrationFlow scatterGather() {
    return IntegrationFlows.from(messageSourceS())
            .scatterGather(scatterer -> scatterer
                            .applySequence(true)
                            .recipientFlow(process1())
                            .recipientFlow(process2()),
                    gatherer -> gatherer.outputProcessor(
                            messageGroup -> {
                              log.info("Message group: {}", messageGroup);
                              return messageGroup.streamMessages().map(Message::getPayload).map(Object::toString).collect(Collectors.joining(";"));
                            }
                    ))
            .get();
  }




}
