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
import io.vertx.config.ConfigRetriever;
import org.meshr.collector.vertx.pubsub.PubsubVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends AbstractVerticle {

    private final static Logger LOG = LoggerFactory.getLogger("MainVerticle.class");

    @Override
    public void start(Promise<Void> promise) throws Exception { 
        LOG.info("MainVerticle deployed");

        ConfigRetriever retriever = ConfigRetriever.create(vertx);
            retriever.getConfig(
                config -> {
                    if (config.failed()) {
                        LOG.info("Config retriever failed.");
                        promise.fail(config.cause());
                    } else {
                        Promise<String> pubsubVerticleDeployment = Promise.promise();
                        vertx.deployVerticle(
                            PubsubVerticle.class.getName(),
                            new DeploymentOptions()
                                .setConfig(config.result()), 
                            pubsubVerticleDeployment
                        );
                        pubsubVerticleDeployment.future().compose(id -> {
                            Promise<String> httpVerticleDeployment = Promise.promise();
                            vertx.deployVerticle(
                                "org.meshr.collector.vertx.http.HttpServerVerticle",
                                new DeploymentOptions()
                                    .setInstances(1)
                                    .setConfig(config.result()),
                                httpVerticleDeployment
                            );
                            return httpVerticleDeployment.future();
                        }).setHandler(ar -> {
                            if (ar.succeeded()) {
                                LOG.info("PubsubVerticle deployed");
                                promise.complete();
                            } else {
                                LOG.error("PubsubVerticle failed to deploy");
                                promise.fail(ar.cause());
                            }
                        });
                    }
            });
    }
}