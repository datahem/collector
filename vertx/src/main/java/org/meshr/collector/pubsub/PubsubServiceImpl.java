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


class PubsubServiceImpl implements PubsubService {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubServiceImpl.class);

    LoadingCache<String, Publisher> publisherCache;
    JsonObject _config;
    WebClient client;
    Random rand;
    String backupTopic;

    PubsubServiceImpl(
        LoadingCache<String, Publisher> publisherCache, 
        JsonObject _config,
        WebClient client,
        Handler<AsyncResult<PubsubService>> readyHandler) {
            this.publisherCache = publisherCache;
            this._config = _config;
            this.client = client;
            this.backupTopic = _config.getString("BACKUP_TOPIC");
            this.rand = new Random();
            readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public PubsubService publishMessage(String payload, Map<String,String> headers, String topic, Handler<AsyncResult<Void>> resultHandler) {
        String uuid = UUID.randomUUID().toString();
        
        //Anonymize IP
        try{
            String ip = headers.getOrDefault("x-forwarded-for", "").split(",")[0];
            if(ip.lastIndexOf(".") != -1){ // IP v4
                headers.put("x-forwarded-for", ip.substring(0, ip.lastIndexOf("."))+".0");
            }else if(ip.lastIndexOf(":") != -1){ // IP v6
                int n = 3;
                String substr = ":";
                int pos = ip.indexOf(substr);
                while (--n > 0 && pos != -1)
                    pos = ip.indexOf(substr, pos + 1);
                headers.put("x-forwarded-for", ip.substring(0, pos)+":::::");
            }
        }catch(StringIndexOutOfBoundsException e){
            LOG.error("IP Anonymization StringIndexOutOfBoundsException: ", e);
        }
        
        try {
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
			    .putAllAttributes(
                    ImmutableMap.<String, String>builder()
                        .putAll(headers)
                        //.put("timestamp", new DateTime(DateTimeZone.UTC).toString())
                        .put("timestamp", Instant.now().toString())
                        .put("source", topic)
                        .put("uuid", uuid)
                        .build()
                    )
                    .setData(ByteString.copyFromUtf8(payload))
                .build();
            ApiFuture<String> topicFuture = publisherCache.get(topic).publish(pubsubMessage);
            ApiFuture<String> backupFuture = publisherCache.get(backupTopic).publish(pubsubMessage);

            ApiFutures.addCallback(topicFuture,
                new ApiFutureCallback<String>() {
                    public void onSuccess(String messageId) {
                        try{
                            ApiFutures.addCallback(backupFuture,
                            new ApiFutureCallback<String>() {
                                public void onSuccess(String messageId) {
                                    resultHandler.handle(Future.succeededFuture());
                                }
                                public void onFailure(Throwable t) {
                                    LOG.error("Failed to publish: ", t);
                                    resultHandler.handle(Future.failedFuture(t));    
                                }
                            }, MoreExecutors.directExecutor());
                        }catch (Exception e) {
                            LOG.error("PubSubClient contextInitialized error ", e);
                        }
                    }
                    public void onFailure(Throwable t) {
                        LOG.error("failed to publish: ", t);
                        resultHandler.handle(Future.failedFuture(t));    
                    }
            }, MoreExecutors.directExecutor());

        } catch (Exception e) {
            LOG.error("PubSubClient contextInitialized error ", e);
        }
        keepAlive();
    return this;
  }

    private void keepAlive(){
        if(_config.getInteger("FREQUENCY") > 0 && rand.nextInt(_config.getInteger("FREQUENCY"))==0){ 
            LOG.info(_config.getInteger("HOST_PORT", 443) + _config.getString("HOST") + _config.getString("HOST_URI"));
            client
                .post(_config.getInteger("HOST_PORT", 443), _config.getString("HOST"), _config.getString("HOST_URI"))
                .ssl(true)
                .putHeader("Content-Type", "application/json")
                .as(BodyCodec.jsonObject())
                .sendJsonObject(new JsonObject()
                    .put("id", _config.getString("PROJECT_ID"))
                    .put("class", this.getClass().getPackage().toString())
                    , ar -> {
                        if(ar.succeeded()) {
                            LOG.info("Keep alive status " + ar.result().statusCode());
                        }else {
                            LOG.warn("Keep alive fail " + ar.cause().getMessage());
                        }
                });        
        }
    }
}