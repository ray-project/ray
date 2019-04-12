/**
 * Copyright 2015, Yahoo Inc. Licensed under the terms of the Apache License 2.0. Please see LICENSE
 * file in the project root for terms.
 */
package org.ray.streaming.benchmark.common.advertising;

class CampaignWindowPair {

  String campaign;
  Window window;

  public CampaignWindowPair(String campaign, Window window) {
    this.campaign = campaign;
    this.window = window;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof CampaignWindowPair) {
      return campaign.equals(((CampaignWindowPair) other).campaign)
          && window.equals(((CampaignWindowPair) other).window);
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;

    result = result * prime + campaign.hashCode();
    result = result * prime + window.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "{ " + campaign + " : " + window.toString() + " }";
  }
}
