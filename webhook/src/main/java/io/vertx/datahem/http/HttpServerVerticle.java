
package io.vertx.datahem.http;

import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.BodyHandler;


import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.vertx.datahem.pubsub.PubsubService;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.stream.Stream;
import java.util.Enumeration;
import java.util.UUID;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/*
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
*/
import java.util.logging.Logger;

/**
 * @author <a href="https://robertsahlin.com/">Robert Sahlin</a>
 */
public class HttpServerVerticle extends AbstractVerticle {

  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
  public static final String CONFIG_PUBSUB_QUEUE = "pubsub.queue";
  public static final String BACKUP_TOPIC = "tmp";
   private PubsubService pubsubService;
    //private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    private final static Logger LOGGER = Logger.getLogger("HttpServerVerticle");
    //private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);
    
    //private static LoadingCache<String, Publisher> publishers;
	//private static CacheLoader<String, Publisher> loader;
	//private static RemovalListener<String, Publisher> removalListener;
  


  @Override
  public void start(Promise<Void> promise) throws Exception {
      LOGGER.info("Creating vertx HTTP server: " + System.currentTimeMillis());

        String pubsubQueue = config().getString(CONFIG_PUBSUB_QUEUE, "pubsub.queue");
        pubsubService = PubsubService.createProxy(vertx, pubsubQueue);

      /*
      loader = new CacheLoader<String, Publisher>() {
				@Override
				public Publisher load(String pubSubTopicId) {
					Publisher publisher = null;
					try{
						ProjectTopicName topic = ProjectTopicName.of(PROJECT_ID, pubSubTopicId);
						publisher = Publisher
							.newBuilder(topic)
							.build();
						//LOG.info("Cache load: " + publisher.getTopicNameString() + ", ref: " + publisher.toString());
					}catch (Exception e) {
						//LOG.error("PubSubClient Connect load error ", e);
					}
					return publisher;
				}
			};
			
			removalListener = new RemovalListener<String, Publisher>() {
				@Override
				public void onRemoval(RemovalNotification<String, Publisher> removal) {
					final Publisher publisher = removal.getValue();
					//LOG.info("Cache remove: " + publisher.getTopicNameString() + ", ref: " + publisher.toString());
					if (publisher != null) {
                        publisher.shutdown();
                        vertx.executeBlocking(promise -> {
                            try{
                                publisher.awaitTermination(1, TimeUnit.SECONDS);
                            }catch(Exception e){
                                //LOG.error("PubSubClient Connect load error ", e);
                            }    
                            promise.complete("");
                        }, 
                        res -> {
                            System.out.println("Publisher terminated");
                        });
					}
				}
			};
			
			publishers = CacheBuilder
				.newBuilder()
				.maximumSize(1000)
				.removalListener(removalListener)
				.expireAfterAccess(60, TimeUnit.SECONDS)
				.build(loader);*/

    HttpServer server = vertx.createHttpServer();

    // tag::apiRouter[]
    Router apiRouter = Router.router(vertx);
    apiRouter.get("/topic/:id").handler(this::apiGetGif);
    //apiRouter.post().handler(BodyHandler.create());
    //apiRouter.post("/topic/:id").handler(this::apiPost);
    // end::apiRouter[]

    int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
    server
      .requestHandler(apiRouter)
      .listen(portNumber, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running on port " + portNumber);
          promise.complete();
        } else {
          LOGGER.info("Could not start a HTTP server: " + ar.cause().toString());
          promise.fail(ar.cause());
        }
      });
  }
/*
    private void apiPost(RoutingContext context) {
        String topic = String.valueOf(context.request().getParam("id"));
        //JsonObject payload = context.getBodyAsJson();
        publishMessage(context.getBodyAsJson().toString(), context.request(), topic);
        context.response().setStatusCode(204);
        context.response().end();
    }
*/
    private void apiGetGif(RoutingContext context) {
        
        Handler<AsyncResult<Void>> handler = reply -> {
            if (reply.succeeded()) {
                context.response().setStatusCode(204);
                context.response().end();
            } else {
                LOGGER.info("apiGetGif handler reply fail: " + reply.cause().toString());
                context.fail(reply.cause());
            }
        };

        String headers = "hello";//context.request().headers().asMap().toString();
        String payload = context.request().query();
        String topic = String.valueOf(context.request().getParam("id"));
        LOGGER.info("apiGet publish message: " + System.currentTimeMillis());
        pubsubService.publishMessage(payload, headers, topic, handler);
       /*
        LOGGER.info("apiGet start: " + System.currentTimeMillis());
        String topic = String.valueOf(context.request().getParam("id"));
        publishMessage(context.request().query(), context.request(), topic);
        context.response().setStatusCode(204);
        context.response().end();
        LOGGER.info("apiGet end: " + System.currentTimeMillis());*/
  }

/*
    private void publishMessage(String payload,  HttpServerRequest request, String topic){
        String uuid = UUID.randomUUID().toString();
        HashMap<String, String> headers = 
			request.headers().names()
			.stream()
			.map(s -> new String[]{s, request.headers().get(s)})
            .collect(HashMap::new, (m,v)->m.put(v[0], v[1]), HashMap::putAll);
        
        try{
            String ip = headers.getOrDefault("X-Forwarded-For", "").split(",")[0];
            if(ip.lastIndexOf(".") != -1){
                headers.put("X-Forwarded-For", ip.substring(0, ip.lastIndexOf("."))+".0");
            }else if(ip.lastIndexOf(":") != -1){
                int n = 3;
                String substr = ":";
                int pos = ip.indexOf(substr);
                while (--n > 0 && pos != -1)
                    pos = ip.indexOf(substr, pos + 1);
                headers.put("X-Forwarded-For", ip.substring(0, pos)+":::::");
            }
        }catch(StringIndexOutOfBoundsException e){
            //LOG.error("collector buildcollectorpayload ip StringIndexOutOfBoundsException", e);
        }
        try {
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
			    .putAllAttributes(
                    ImmutableMap.<String, String>builder()
                        .putAll(headers)
                        .put("timestamp", new DateTime(DateTimeZone.UTC).toString())
                        .put("source", topic)
                        .put("uuid", uuid)
                        .build()
                    )
                .setData(ByteString.copyFromUtf8(payload))
                .build();
            LOGGER.info("apiGet publish start: " + System.currentTimeMillis());
            ApiFutures.addCallback(publishers.get(topic).publish(pubsubMessage),
                new ApiFutureCallback<String>() {
                    public void onSuccess(String messageId) {
                        LOGGER.info("apiGet publish success: " + System.currentTimeMillis());
                        System.out.println("published with message id: " + messageId);}
                    public void onFailure(Throwable t) {System.out.println("failed to publish: " + t);}
            }, MoreExecutors.directExecutor());
            ApiFutures.addCallback(publishers.get(BACKUP_TOPIC).publish(pubsubMessage),
                new ApiFutureCallback<String>() {
                    public void onSuccess(String messageId) {System.out.println("published with message id: " + messageId);}
                    public void onFailure(Throwable t) {System.out.println("failed to publish: " + t);}
            }, MoreExecutors.directExecutor());
        } catch (Exception e) {
            //LOG.error("PubSubClient contextInitialized error ", e);
        }
		
	}*/

}