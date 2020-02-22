package org.meshr.collector.vertx.pubsub;

/*
 * Copyright (c) 2020 Robert Sahlin
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file.
 */

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
//import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ServiceBinder;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.json.JsonObject;

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
import io.vertx.ext.web.client.WebClient;
import io.vertx.core.http.RequestOptions;


public class PubsubVerticle extends AbstractVerticle {

    public static final String CONFIG_PUBSUB_QUEUE = "pubsub.queue";
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    private static LoadingCache<String, Publisher> publishers;
    private static CacheLoader<String, Publisher> loader;
    private static RemovalListener<String, Publisher> removalListener;
    private WebClient client;
    private RequestOptions requestOptions = new RequestOptions();
    private final static Logger LOGGER = Logger.getLogger("PubsubVerticle");

  @Override
  public void start(Promise<Void> promise) throws Exception {
      LOGGER.info(PROJECT_ID);

      loader = new CacheLoader<String, Publisher>() {
				@Override
				public Publisher load(String pubSubTopicId) {
					Publisher publisher = null;
					try{
                        LOGGER.info("PROJECT_ID: " + PROJECT_ID);
                        LOGGER.info("pubSubTopicID: " + pubSubTopicId);
						ProjectTopicName topic = ProjectTopicName.of(PROJECT_ID, pubSubTopicId);
						publisher = Publisher
							.newBuilder(topic)
							.build();
						LOGGER.info("Cache load: " + publisher.getTopicNameString() + ", ref: " + publisher.toString());
					}catch (Exception e) {
						LOGGER.info("PubSubClient Connect load error " + e.toString());
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
                                LOGGER.info("PubSubClient Connect load error " + e);
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
				.expireAfterAccess(600, TimeUnit.SECONDS)
				.build(loader);
            
            client = WebClient.create(vertx);

            ConfigRetriever retriever = ConfigRetriever.create(vertx);
            retriever.getConfig(
                config -> {
                    if (config.failed()) {
                        promise.fail(config.cause());
                    } else {
                        /*
                        String host = config.result().getString("HOST", "8080-dot-3511156-dot-devshell.appspot.com");
                        LOGGER.info("HOST: " + host);
                        int port = config.result().getInteger("PORT", 8080);
                        LOGGER.info("PORT: " + port);
                        String uri = config.result().getString("URI", "/optimize/default/topic/tmp");
                        LOGGER.info("URI: " + uri);*/
                        //requestOptions.setHost(host);
                            //.setSsl(true)
                            //.setPort(port)
                            //.setURI(uri);
                        //LOGGER.info("PubsubService.create BACKUP_TOPIC: " + config.result().getString("BACKUP_TOPIC"));
                        JsonObject jsconfig = new JsonObject();
                        jsconfig.put("foo","bar");

                        PubsubService.create(
                            publishers,
                            config.result().getString("BACKUP_TOPIC", "tmp"),
                            jsconfig,
                            PROJECT_ID,
                            client,
                            requestOptions
                                .setHost(config.result().getString("HOST", "8080-dot-3511156-dot-devshell.appspot.com"))
                                //.setSsl(true)
                                .setPort(config.result().getInteger("PORT", 8080))
                                .setURI(config.result().getString("URI", "/optimize/default/topic/tmp")),
                            ready -> {
                                if (ready.succeeded()) {
                                    //LOGGER.info("PubsubService.create succeded: " + System.currentTimeMillis());
                                    ServiceBinder binder = new ServiceBinder(vertx);
                                    binder
                                        .setAddress(CONFIG_PUBSUB_QUEUE)
                                        .register(PubsubService.class, ready.result());
                                    promise.complete();
                                } else {
                                    //LOGGER.info("PubsubService.create fail: " + System.currentTimeMillis());
                                    promise.fail(ready.cause());
                                }
                            }
                        );
                    }
                }
            );
    }
}