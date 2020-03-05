package org.ray.api.options;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The options for creating a placement group.
 *
 * A placement group is used to preallocate some resources across the cluster according to a
 * specific placement strategy. It is composed of one or more bundles.
 *
 * When a placement group is created, bundles in it can be used to create Actors. We can set a
 * bundle to `ActorCreationOptions` to tell Ray that we want to allocate resources in the bundle for
 * this Actor.
 */
public class PlacementGroupOptions {

  /**
   * A unique string name of a placement group.
   */
  public final String name;

  /**
   * The scheduling units in a placement group. These units (bundles) are scheduled according to the
   * `strategy` field.
   */
  public final List<BundleOptions> bundles;

  /**
   * The strategy just works for bundles. Resource blocks in a bundle have their own fixed
   * strategy.
   */
  public final PlacementStrategy strategy;

  private PlacementGroupOptions(String name, List<BundleOptions> bundles,
      PlacementStrategy strategy) {
    this.name = name;
    this.bundles = bundles;
    this.strategy = strategy;
  }

  /**
   * The inner class for building PlacementGroupOptions.
   */
  public static class Builder {

    private String name = "";
    private List<BundleOptions> bundles = new ArrayList<>();
    private PlacementStrategy strategy;

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Add a COMMON bundle to the placement group. A COMMON bundle contains some resources that will
     * be STRICTLY allocated in a Ray Node. It is used to pack not-too-many resources close
     * together.
     *
     * @param resources Resources contained in this bundle.
     */
    public Builder addBundle(Map<String, Double> resources) {
      this.bundles
        .add(new BundleOptions.Builder().setUnitResources(resources).createBundleOptions());
      return this;
    }

    /**
     * Add a BIG bundle to the placement group. A BIG bundle contains some resource blocks. A
     * resource block will be STRICTLY allocated in a Ray Node, while different resource blocks MAY
     * OR MAY NOT be in different Ray Nodes. It is used to pack a large amount of resources close
     * together.
     *
     * TODO(yuyiming): This API may not be exposed to users in this version, because it introduces
     * the resource block as another scheduling layer, which increases users' learning costs.
     *
     * @param unitResources Resources contained in a resource block.
     * @param unitCount Number of resource blocks.
     */
    public Builder addBundle(Map<String, Double> unitResources, int unitCount) {
      this.bundles.add(
          new BundleOptions.Builder().setUnitResources(unitResources).setUnitCount(unitCount)
              .createBundleOptions());
      return this;
    }

    public Builder setPlacementStrategy(PlacementStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public PlacementGroupOptions createPlacementGroupOptions() {
      return new PlacementGroupOptions(name, bundles, strategy);
    }
  }
}
