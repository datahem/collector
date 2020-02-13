package io.vertx.datahem.pubsub;

/*
 * Copyright (c) 2020 Robert Sahlin
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file.
 */

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.logging.Logger;
import io.vertx.core.CompositeFuture;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.google.common.cache.LoadingCache;
import com.google.cloud.pubsub.v1.Publisher;

import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;
import java.util.UUID;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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



// tag::implementation[]
class PubsubServiceImpl implements PubsubService {

  //private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseServiceImpl.class);
    private final static Logger LOGGER = Logger.getLogger("PubsubServiceImpl");

    LoadingCache<String, Publisher> publisherCache;
    String backupTopic;

    PubsubServiceImpl(LoadingCache<String, Publisher> publisherCache, String backupTopic, Handler<AsyncResult<PubsubService>> readyHandler) {
        this.publisherCache = publisherCache;
        this.backupTopic = backupTopic;
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public PubsubService publishMessage(String payload, String headers, String topic, Handler<AsyncResult<Void>> resultHandler) {
        LOGGER.info("PubsubService.publishMessage: " + System.currentTimeMillis());
        String uuid = UUID.randomUUID().toString();
        
        /*
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
        */
        try {
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
			    /*.putAllAttributes(
                    ImmutableMap.<String, String>builder()
                        .putAll(headers)
                        .put("timestamp", new DateTime(DateTimeZone.UTC).toString())
                        .put("source", topic)
                        .put("uuid", uuid)
                        .build()
                    )*/
                .setData(ByteString.copyFromUtf8(payload))
                .build();
            LOGGER.info("apiGet publish start: " + System.currentTimeMillis());
            ApiFuture<String> topicFuture = publisherCache.get(topic).publish(pubsubMessage);
            ApiFuture<String> backupFuture = publisherCache.get(backupTopic).publish(pubsubMessage);

            ApiFutures.addCallback(topicFuture,
                new ApiFutureCallback<String>() {
                    public void onSuccess(String messageId) {
                        LOGGER.info("apiGet publish success: " + System.currentTimeMillis());
                        System.out.println("published with message id: " + messageId);
                        try{
                            ApiFutures.addCallback(backupFuture,
                            new ApiFutureCallback<String>() {
                                public void onSuccess(String messageId) {
                                    LOGGER.info("apiGet publish success: " + System.currentTimeMillis());
                                    System.out.println("published with message id: " + messageId);
                                    resultHandler.handle(Future.succeededFuture());
                                }
                                public void onFailure(Throwable t) {
                                    System.out.println("failed to publish: " + t);
                                    resultHandler.handle(Future.failedFuture(t));    
                                }
                            }, MoreExecutors.directExecutor());
                        }catch (Exception e) {
                            //LOG.error("PubSubClient contextInitialized error ", e);
                        }
                        //resultHandler.handle(Future.succeededFuture());
                    }
                    public void onFailure(Throwable t) {
                        System.out.println("failed to publish: " + t);
                        resultHandler.handle(Future.failedFuture(t));    
                    }
            }, MoreExecutors.directExecutor());

        } catch (Exception e) {
            //LOG.error("PubSubClient contextInitialized error ", e);
        }

    return this;
  }

}
// end::implementation[]