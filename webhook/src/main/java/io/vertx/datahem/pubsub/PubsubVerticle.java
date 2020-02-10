package io.vertx.datahem.pubsub;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
//import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.Properties;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.ProjectTopicName;
import java.util.concurrent.TimeUnit;
import com.google.cloud.ServiceOptions;


// tag::dbverticle[]
public class PubsubVerticle extends AbstractVerticle {

    public static final String CONFIG_PUBSUB_QUEUE = "pubsub.queue";
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    private static LoadingCache<String, Publisher> publishers;
    private static CacheLoader<String, Publisher> loader;
    private static RemovalListener<String, Publisher> removalListener;
    private final static Logger LOGGER = Logger.getLogger("PubsubVerticle");

  @Override
  public void start(Promise<Void> promise) throws Exception {

      loader = new CacheLoader<String, Publisher>() {
				@Override
				public Publisher load(String pubSubTopicId) {
					Publisher publisher = null;
					try{
						ProjectTopicName topic = ProjectTopicName.of(PROJECT_ID, pubSubTopicId);
						publisher = Publisher
							.newBuilder(topic)
							.build();
						LOGGER.info("Cache load: " + publisher.getTopicNameString() + ", ref: " + publisher.toString());
					}catch (Exception e) {
						//LOGGER.info("PubSubClient Connect load error ", e);
					}
					return publisher;
				}
			};
			
			removalListener = new RemovalListener<String, Publisher>() {
				@Override
				public void onRemoval(RemovalNotification<String, Publisher> removal) {
					final Publisher publisher = removal.getValue();
					LOGGER.info("Cache remove: " + publisher.getTopicNameString() + ", ref: " + publisher.toString());
					if (publisher != null) {
                        publisher.shutdown();
                        vertx.executeBlocking(promise -> {
                            try{
                                publisher.awaitTermination(1, TimeUnit.SECONDS);
                            }catch(Exception e){
                                //LOGGER.info("PubSubClient Connect load error ", e);
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
				.build(loader);

        PubsubService.create(publishers, ready -> {
            if (ready.succeeded()) {
                LOGGER.info("PubsubService.create succeded: " + System.currentTimeMillis());
                ServiceBinder binder = new ServiceBinder(vertx);
                binder
                    .setAddress(CONFIG_PUBSUB_QUEUE)
                    .register(PubsubService.class, ready.result());
                promise.complete();
            } else {
                LOGGER.info("PubsubService.create fail: " + System.currentTimeMillis());
                promise.fail(ready.cause());
            }
        });
        LOGGER.info("PubsubVerticle started: " + System.currentTimeMillis());
    }
}
// end::dbverticle[]