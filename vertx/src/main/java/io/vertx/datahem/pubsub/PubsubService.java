package io.vertx.datahem.pubsub;

/*
 * Copyright (c) 2020 Robert Sahlin
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file.
 */

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.core.http.RequestOptions;

import com.google.common.cache.LoadingCache;
import com.google.cloud.pubsub.v1.Publisher;

import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * @author <a href="https://julien.ponge.org/">Julien Ponge</a>
 */
// tag::interface[]
@ProxyGen
@VertxGen
public interface PubsubService {

  @Fluent
  PubsubService publishMessage(String payload, Map<String,String>headers, String topic, Handler<AsyncResult<Void>> resultHandler);

  // end::interface[]

  // tag::create[]
    @GenIgnore
    static PubsubService create(
        LoadingCache<String, Publisher> publisherCache, 
        String backupTopic,
        WebClient client,
        RequestOptions requestOptions, 
        Handler<AsyncResult<PubsubService>> readyHandler) {
            return new PubsubServiceImpl(publisherCache, backupTopic, client, requestOptions, readyHandler);
        }
  // end::create[]

  // tag::proxy[]
  @GenIgnore
  static PubsubService createProxy(Vertx vertx, String address) {
    return new PubsubServiceVertxEBProxy(vertx, address);
  }
  // end::proxy[]
}