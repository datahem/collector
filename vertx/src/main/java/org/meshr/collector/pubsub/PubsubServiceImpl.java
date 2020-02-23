package org.meshr.collector.vertx.pubsub;

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
//import java.util.logging.Logger;
import io.vertx.core.CompositeFuture;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.core.http.RequestOptions;
import io.vertx.core.http.HttpMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.ServiceOptions;

import com.google.common.cache.LoadingCache;
import com.google.cloud.pubsub.v1.Publisher;

import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;
import java.util.UUID;
import com.google.common.collect.ImmutableMap;
//import org.joda.time.DateTime;
//import org.joda.time.DateTimeZone;
import java.time.Instant;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(PubsubServiceImpl.class);
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    //private final static Logger LOGGER = Logger.getLogger("PubsubServiceImpl");

    LoadingCache<String, Publisher> publisherCache;
    String backupTopic;
    JsonObject jsconfig;
    String projectId;
    WebClient client;
    RequestOptions requestOptions;
    Random rand;

    PubsubServiceImpl(
        LoadingCache<String, Publisher> publisherCache, 
        //String backupTopic, 
        JsonObject jsconfig,
        //String projectId,
        WebClient client,
        //RequestOptions requestOptions, 
        Handler<AsyncResult<PubsubService>> readyHandler) {
            this.publisherCache = publisherCache;
            //this.backupTopic = backupTopic;
            this.jsconfig = jsconfig;
            //this.projectId = projectId;
            this.client = client;
            //this.requestOptions = requestOptions;
            this.rand = new Random();
            readyHandler.handle(Future.succeededFuture(this));
            this.backupTopic = jsconfig.getString("BACKUP_TOPIC");
            this.projectId = jsconfig.getString("PROJECT_ID");
            this.requestOptions = new RequestOptions()
            //requestOptions
                .setHost(jsconfig.getString("HOST"))
                .setPort(jsconfig.getInteger("HOST_PORT"))
                .setURI(jsconfig.getString("HOST_URI"));
    }

    @Override
    public PubsubService publishMessage(String payload, Map<String,String> headers, String topic, Handler<AsyncResult<Void>> resultHandler) {
        //LOGGER.info("PubsubService.publishMessage: " + System.currentTimeMillis());
        String uuid = UUID.randomUUID().toString();
        //LOGGER.info("PubsubService.publishMessage: " + headers.toString());
        LOGGER.info("topic: " + topic);
        
        
        try{
            String ip = headers.getOrDefault("x-forwarded-for", "").split(",")[0];
            if(ip.lastIndexOf(".") != -1){
                headers.put("x-forwarded-for", ip.substring(0, ip.lastIndexOf("."))+".0");
            }else if(ip.lastIndexOf(":") != -1){
                int n = 3;
                String substr = ":";
                int pos = ip.indexOf(substr);
                while (--n > 0 && pos != -1)
                    pos = ip.indexOf(substr, pos + 1);
                headers.put("x-forwarded-for", ip.substring(0, pos)+":::::");
            }
        }catch(StringIndexOutOfBoundsException e){
            //LOG.error("collector buildcollectorpayload ip StringIndexOutOfBoundsException", e);
        }
        
        try {
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
			    /*.putAllAttributes(
                    ImmutableMap.<String, String>builder()
                        .putAll(headers)
                        //.put("timestamp", new DateTime(DateTimeZone.UTC).toString())
                        //.put("timestamp", Instant.now().toString())
                        .put("source", topic)
                        .put("uuid", uuid)
                        .build()
                    )*/
                    .setData(ByteString.copyFromUtf8(payload))
                    //.setData(ByteString.copyFromUtf8("hello"))
                .build();
            //LOGGER.info("apiGet publish start: " + System.currentTimeMillis());
            ApiFuture<String> topicFuture = publisherCache.get(topic).publish(pubsubMessage);
            ApiFuture<String> backupFuture = publisherCache.get(backupTopic).publish(pubsubMessage);

            ApiFutures.addCallback(topicFuture,
                new ApiFutureCallback<String>() {
                    public void onSuccess(String messageId) {
                        //LOGGER.info("apiGet publish success: " + System.currentTimeMillis());
                        System.out.println("published with message id: " + messageId);
                        try{
                            ApiFutures.addCallback(backupFuture,
                            new ApiFutureCallback<String>() {
                                public void onSuccess(String messageId) {
                                    //LOGGER.info("apiGet publish success: " + System.currentTimeMillis());
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
            LOGGER.error("PubSubClient contextInitialized error ", e);
        }
        keepAlive();
    return this;
  }

    private void keepAlive(){
        LOGGER.info("Enter ping()");
        if(rand.nextInt(2)==0){ 
            LOGGER.info("Pinging");
            LOGGER.info("projectId: " + projectId );
            LOGGER.info("requestoptions: " + requestOptions.toJson().toString() );
            client
                //.request(HttpMethod.POST,requestOptions)
                //.request(HttpMethod.GET,requestOptions)
                //.get(80, jsconfig.getString("HOST"), jsconfig.getString("HOST_URI"))
                //.addQueryParam("package", "io.vertx.datahem.pubsub")
                //.addQueryParam("project", ServiceOptions.getDefaultProjectId())
                //.send(ar -> {
                .post(jsconfig.getString("HOST_PORT"), jsconfig.getString("HOST"), jsconfig.getString("HOST_URI"))
                .ssl(true)
                .putHeader("Content-Type", "application/json")
                .as(BodyCodec.jsonObject())
                .sendJsonObject(new JsonObject()
                    .put("projectID", "projectId")
                    .put("package", "org.meshr.collector.vertx.pubsub")
                    //.put("version", jsconfig.getString("VERSION"))
                    , ar -> {
                        if(ar.succeeded()) {
                            LOGGER.info("Keep alive response status code " + ar.result().statusCode());
                            LOGGER.info("Keep alive response status code " + ar.result().statusMessage());
                            LOGGER.info("Keep alive response Location header " + ar.result().getHeader("Location"));
                        }else {
                            LOGGER.error("Something went wrong " + ar.cause().getMessage());
                        }
                });
        }
    }
}