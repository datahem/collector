package org.meshr.collector.vertx;

/*
 * Copyright (c) 2020 Robert Sahlin
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE file.
 */

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.meshr.collector.vertx.pubsub.PubsubVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends AbstractVerticle {

    private final static Logger LOGGER = LoggerFactory.getLogger("MainVerticle.class");

    @Override
    public void start(Promise<Void> promise) throws Exception { 
        LOGGER.info("MainVerticle deployed");
        Promise<String> pubsubVerticleDeployment = Promise.promise();
        vertx.deployVerticle(new PubsubVerticle(), pubsubVerticleDeployment);

        pubsubVerticleDeployment.future().compose(id -> {
            Promise<String> httpVerticleDeployment = Promise.promise();
            vertx.deployVerticle(
                "org.meshr.collector.vertx.http.HttpServerVerticle",
                new DeploymentOptions().setInstances(1),
            httpVerticleDeployment);
            return httpVerticleDeployment.future();
        }).setHandler(ar -> {
            if (ar.succeeded()) {
                LOGGER.info("PubsubVerticle deployed");
                promise.complete();
            } else {
                LOGGER.error("PubsubVerticle failed to deploy");
                promise.fail(ar.cause());
            }
        });
        
    }
}