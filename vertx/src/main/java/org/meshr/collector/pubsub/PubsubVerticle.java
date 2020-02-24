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
//import java.util.logging.Logger;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(PubsubVerticle.class);

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
						LOG.info("Cache load: " + publisher.getTopicNameString() + ", ref: " + publisher.toString());
					}catch (Exception e) {
						LOG.info("PubSubClient Connect load error " + e.toString());
					}
					return publisher;
				}
			};
			
			removalListener = new RemovalListener<String, Publisher>() {
				@Override
				public void onRemoval(RemovalNotification<String, Publisher> removal) {
					final Publisher publisher = removal.getValue();
					LOG.info("Cache remove: " + publisher.getTopicNameString() + ", ref: " + publisher.toString());
					if (publisher != null) {
                        publisher.shutdown();
                        vertx.executeBlocking(promise -> {
                            try{
                                publisher.awaitTermination(1, TimeUnit.SECONDS);
                            }catch(Exception e){
                                LOG.error("PubSubClient Connect load error " + e);
                            }    
                            promise.complete("");
                        }, 
                        res -> {
                            LOG.info("Publisher terminated");
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
            
            //client = WebClient.create(vertx);
            PubsubService.create(
                            publishers,
                            config().put("PROJECT_ID", PROJECT_ID),
                            WebClient.create(vertx),
                            //client,
                            ready -> {
                                if (ready.succeeded()) {
                                    LOG.info("PubsubService created.");
                                    ServiceBinder binder = new ServiceBinder(vertx);
                                    binder
                                        .setAddress(CONFIG_PUBSUB_QUEUE)
                                        .register(PubsubService.class, ready.result());
                                    promise.complete();
                                } else {
                                    LOG.error("PubsubService failed to create.");
                                    promise.fail(ready.cause());
                                }
                            }
                        );
            /*
            ConfigRetriever retriever = ConfigRetriever.create(vertx);
            
            retriever.getConfig(
                config -> {
                    if (config.failed()) {
                        LOG.info("Config retriever failed.");
                        promise.fail(config.cause());
                    } else {
                        JsonObject _config = new JsonObject();
                        _config.put("BACKUP_TOPIC",config.result().getString("BACKUP_TOPIC", "backup"));
                        _config.put("HOST", config.result().getString("HOST"));
                        _config.put("HOST_PORT", config.result().getInteger("HOST_PORT", 8080));
                        _config.put("HOST_URI", config.result().getString("HOST_URI"));
                        _config.put("FREQUENCY", config.result().getInteger("FREQUENCY", 0));
                        _config.put("PROJECT_ID", PROJECT_ID);
                        _config.put("BODY", config.result().getString("BODY"));

                        PubsubService.create(
                            publishers,
                            _config,
                            client,
                            ready -> {
                                if (ready.succeeded()) {
                                    LOG.info("PubsubService created.");
                                    ServiceBinder binder = new ServiceBinder(vertx);
                                    binder
                                        .setAddress(CONFIG_PUBSUB_QUEUE)
                                        .register(PubsubService.class, ready.result());
                                    promise.complete();
                                } else {
                                    LOG.error("PubsubService failed to create.");
                                    promise.fail(ready.cause());
                                }
                            }
                        );
                    }
                }
            );*/
    }
}