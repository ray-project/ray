/**
 * Alipay.com Inc. Copyright (c) 2004-2019 All Rights Reserved.
 */
package org.ray.deploy.rps.k8s.model;

import io.fabric8.kubernetes.client.CustomResource;
import java.util.HashMap;
import java.util.Map;

/**
 * BaseCustomResource
 *
 * @author qstar
 * @version : BaseCustomResource.java, v 0.1 2019年05月17日 10:53 qstar Exp $
 */
public class BaseCustomResource extends CustomResource {
    public static final String LABEL_POOL = "mandatory.k8s.alipay.com/app-logic-pool";
    public static final String LABEL_USER = "ray_cluster_user";

    /**
     *  With caution here it should not be `getName` for the reason of serialization tricky
     */
    public String doGetName() {
        return this.getMetadata().getName();
    }

    public void setName(String name) {
        this.getMetadata().setName(name);
    }

    public String doGetNamespace() {
        return this.getMetadata().getNamespace();
    }

    public void setNamespace(String namespace) {
        this.getMetadata().setNamespace(namespace);
    }

    public String doGetPool() {
        if (this.getMetadata().getLabels() == null) {
            return null;
        } else {
            return this.getMetadata().getLabels().get(LABEL_POOL);
        }
    }

    public void setPool(String pool) {
        if (pool != null && !pool.isEmpty()) {
            if (this.getMetadata().getLabels() == null) {
                this.getMetadata().setLabels(new HashMap<>());
            }
            this.getMetadata().getLabels().put(LABEL_POOL, pool);
        }
    }

    public String doGetUser() {
        if (this.getMetadata().getLabels() == null) {
            return null;
        } else {
            return this.getMetadata().getLabels().get(LABEL_USER);
        }
    }

    public void setUser(String user) {
        if (user != null && !user.isEmpty()) {
            if (this.getMetadata().getLabels() == null) {
                this.getMetadata().setLabels(new HashMap<>());
            }
            this.getMetadata().getLabels().put(LABEL_USER, user);
        }
    }

    public void putLabels(Map<String, String> labels) {
        if (labels == null) {
            return;
        }
        if (this.getMetadata().getLabels() == null) {
            this.getMetadata().setLabels(new HashMap<>());
        }
        this.getMetadata().getLabels().putAll(labels);
    }
}