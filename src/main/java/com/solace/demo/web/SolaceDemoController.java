package com.solace.demo.web;

import com.solace.demo.model.Response;
import com.solacesystems.jcsmp.*;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import reactor.core.publisher.Mono;
import javax.annotation.PostConstruct;

import java.time.Duration;

@Slf4j
@RestController
@RequestMapping(value = "/api")
public class SolaceDemoController {
  
  private static final Logger log = LoggerFactory.getLogger(SolaceDemoController.class);
//   final Topic topic = JCSMPFactory.onlyInstance().createTopic("demo/core/rest/req"); // not used, use topic from URL path
  final Queue replyQ = JCSMPFactory.onlyInstance().createQueue("REPLY_Q");

  private static final String RESILIENCE4J_INSTANCE_NAME = "example";
  private static final String FALLBACK_METHOD = "fallback";
  
    // Solace's parameters

	@Value("${SOL_URL:tcp://localhost:55555}")
	private String solurl;

	@Value("${SOL_USER:default}")
	private String username;

	@Value("${SOL_PASS:default}")
	private String password;

	@Value("${SOL_VPN:default}")
	private String vpn;

	private JCSMPProperties properties = new JCSMPProperties();
	private JCSMPSession session;

	/** Anonymous inner-class for handling publishing events */
	@SuppressWarnings("unused")
	XMLMessageProducer producer;
    Requestor requestor;
	XMLMessageConsumer consumer;
	TextMessage request;

	//Time to wait for a reply message before timing out
	final int responseMsgTimeoutMs = 5000;



    @GetMapping(
        value = "/solace/{topicStr}",
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    @CircuitBreaker(name = RESILIENCE4J_INSTANCE_NAME, fallbackMethod = FALLBACK_METHOD)
    @TimeLimiter(name = RESILIENCE4J_INSTANCE_NAME, fallbackMethod = FALLBACK_METHOD)
    public Mono<Response<Boolean>> testSolace(@RequestBody String body, @PathVariable String topicStr) {
    // public String testSolace(@RequestBody String body, @PathVariable String topicStr) {
		String respStr = "";
        Topic topic = JCSMPFactory.onlyInstance().createTopic(topicStr);

		try {
			request.setText(body);
			request.setReplyTo(replyQ);

			BytesXMLMessage reply = requestor.request(request, responseMsgTimeoutMs, topic);

			// Process the reply
			if (reply != null) {
				if (reply instanceof TextMessage) {
					respStr = ((TextMessage) reply).getText();
					log.debug("TextMessage response received: " + respStr);
				}
				else if (reply instanceof StreamMessage) {
					respStr = ((StreamMessage) reply).dump();
					log.debug("StreamMessage response received: " + respStr);
				}
				else {
					log.debug("Response Message Dump:");
                    log.debug(reply.dump());
				}
			}
            else {
                log.debug("Reply is null");
            }

			reply = null;

		} catch (JCSMPRequestTimeoutException e) {
			log.error("Failed to receive a reply in " + responseMsgTimeoutMs + " msecs");
            return Mono.just(toOkResponse())
                .delayElement(Duration.ofSeconds(5));
		} catch (JCSMPException e) {
			log.error("JCSMPException: " + e.getMessage());
            return Mono.error(new RuntimeException("error"));
		}

        return Mono.just(toOkResponse());
    }

  @GetMapping(
      value = "/timeout/{timeout}",
      produces = MediaType.APPLICATION_JSON_VALUE
  )
  @CircuitBreaker(name = RESILIENCE4J_INSTANCE_NAME, fallbackMethod = FALLBACK_METHOD)
  @TimeLimiter(name = RESILIENCE4J_INSTANCE_NAME, fallbackMethod = FALLBACK_METHOD)
  public Mono<Response<Boolean>> timeout(@PathVariable int timeout) {
    return Mono.just(toOkResponse())
        .delayElement(Duration.ofSeconds(timeout));
  }

  @GetMapping(
      value = "/delay/{delay}",
      produces = MediaType.APPLICATION_JSON_VALUE
  )
  @CircuitBreaker(name = RESILIENCE4J_INSTANCE_NAME, fallbackMethod = FALLBACK_METHOD)
  public Mono<Response<Boolean>> delay(@PathVariable int delay) {
    return Mono.just(toOkResponse())
        .delayElement(Duration.ofSeconds(delay));
  }

  @GetMapping(
      value = "/error/{valid}",
      produces = MediaType.APPLICATION_JSON_VALUE
  )
  @CircuitBreaker(name = RESILIENCE4J_INSTANCE_NAME, fallbackMethod = FALLBACK_METHOD)
  public Mono<Response<Boolean>> error(@PathVariable boolean valid) {
    return Mono.just(valid)
        .flatMap(this::toOkResponse);
  }
  
  public Mono<Response<Boolean>> fallback(Exception ex) {
    return Mono.just(toResponse(HttpStatus.INTERNAL_SERVER_ERROR, Boolean.FALSE))
        .doOnNext(result -> log.warn("fallback executed"));
  }
  
  private Mono<Response<Boolean>> toOkResponse(boolean valid) {
    if (!valid) {
      return Mono.just(toOkResponse());
    }
    return Mono.error(new RuntimeException("error"));
  }
  
  private Response<Boolean> toOkResponse() {
    return toResponse(HttpStatus.OK, Boolean.TRUE);
  }

  private Response<Boolean> toResponse(HttpStatus httpStatus, Boolean result) {
    return Response.<Boolean>builder()
        .code(httpStatus.value())
        .status(httpStatus.getReasonPhrase())
        .data(result)
        .build();
  }

    @PostConstruct
    private void init() throws JCSMPException {

        // Instantiates the SMF stuff at the initiation, reuse for each REST requests later
        // Create a JCSMP Session
        properties.setProperty(JCSMPProperties.HOST, solurl);     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, username); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME, vpn); // message-vpn
        properties.setProperty(JCSMPProperties.PASSWORD, password); // client-password
        session =  JCSMPFactory.onlyInstance().createSession(properties);
        session.connect();

        //This will have the session create the producer and consumer required
        //by the Requestor used below.

        producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void responseReceived(String messageID) {
                log.info("Producer received response for msg: " + messageID);
            }
            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
                log.error("Producer received error for msg: " + messageID + " - " + timestamp + " - " + e);
            }
        });

        consumer = session.getMessageConsumer((XMLMessageListener)null);
        consumer.start();
        request = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    	requestor = session.createRequestor();

    }
}