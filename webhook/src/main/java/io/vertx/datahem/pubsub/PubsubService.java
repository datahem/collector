package io.vertx.datahem.pubsub;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import com.google.common.cache.LoadingCache;
import com.google.cloud.pubsub.v1.Publisher;

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
  PubsubService publishMessage(String payload, String headers, String topic, Handler<AsyncResult<Void>> resultHandler);

  // end::interface[]

  // tag::create[]
  @GenIgnore
  static PubsubService create(LoadingCache<String, Publisher> publisherCache, Handler<AsyncResult<PubsubService>> readyHandler) {
    return new PubsubServiceImpl(publisherCache, readyHandler);
  }
  // end::create[]

  // tag::proxy[]
  @GenIgnore
  static PubsubService createProxy(Vertx vertx, String address) {
    return new PubsubServiceVertxEBProxy(vertx, address);
  }
  // end::proxy[]
}