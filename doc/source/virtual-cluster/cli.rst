Virtual Cluster Management
===========================

.. _virtual-cluster-cli:

Create a Virtual Cluster
------------------------

**Description:**

Creates a new Virtual Cluster with specified parameters, allowing you to define clusters with specific replica distributions and divisibility settings.

**Command:**

`ray vcluster create [OPTIONS]`

**Options:**

.. list-table::
    :widths: 20 16 16 16 60
    :header-rows: 1

    * - Option
      - Type
      - Default
      - Required
      - Description
    * - --address TEXT
      - str
      - None
      - NO
      - Specifies the Ray cluster address. If not provided, the **RAY_ADDRESS** environment variable is used.
    * - --id TEXT
      - str
      - N/A
      - YES
      - Assigns a unique identifier to the Virtual Cluster being created.
    * - --divisible
      - bool
      - False
      - NO
      - Determines if the Virtual Cluster is divisible into smaller logical or job clusters.
    * - --replica-sets TEXT
      - dict
      - N/A
      - YES
      - JSON-serialized dictionary defining the replica sets for the cluster (e.g., `{"group1":2,"group2":3}`, `group1` and `group2` correspond to the node type names passed via the `RAY_NODE_TYPE_NAME` environment variable).

Usage Examples
~~~~~~~~~~~~~~

**Example:** Creating a Divisible Virtual Cluster

`ray vcluster create --id logical1 --divisible --replica-sets '{"group2":1}'`

**Output:**

`Virtual cluster 'logical1' created successfully`

Update a Virtual Cluster
------------------------

**Description:**

Update an existing Virtual Cluster with specified parameters.

**Command:**

`ray vcluster update [OPTIONS]`

**Options:**

.. list-table::
    :widths: 20 16 16 16 60
    :header-rows: 1

    * - Option
      - Type
      - Default
      - Required
      - Description
    * - --address TEXT
      - str
      - None
      - NO
      - Specifies the Ray cluster address. If not provided, the **RAY_ADDRESS** environment variable is used.
    * - --id TEXT
      - str
      - N/A
      - YES
      - Assigns a unique identifier to the Virtual Cluster being created.
    * - --divisible
      - bool
      - False
      - NO
      - Determines if the Virtual Cluster is divisible into smaller logical or job clusters.
    * - --replica-sets TEXT
      - dict
      - N/A
      - YES
      - JSON-serialized dictionary defining the replica sets for the cluster (e.g., `{"group1":2,"group2":3}`, `group1` and `group2` correspond to the node type names passed via the `RAY_NODE_TYPE_NAME` environment variable).
    * - --revision INTEGER
      - int
      - 0
      - NO
      - Indicates the revision number for updating the Virtual Cluster.

Usage Examples
~~~~~~~~~~~~~~

**Example 1:** Updating a Divisible Virtual Cluster

`ray vcluster update --id logical1 --divisible --replica-sets '{"group2":2}'`

**Output:**

`Virtual cluster 'logical1' updated successfully`

**Example 2:** Handling Updating Failure Due to Incorrect Revision

`ray vcluster update --id logical1 --divisible --replica-sets '{"group1":2}' --revision 2`

**Output:**

`Failed to update virtual cluster 'logical1': The revision (2) is expired, the latest revision of the virtual cluster logical1 is 1736911613521214948`

Remove a Virtual Cluster
------------------------

**Description:**

Removes an existing Virtual Cluster by its unique identifier from your Ray environment.

**Command:**

`ray vcluster remove [OPTIONS] <virtual-cluster-id>`

**Options:**

.. list-table::
    :widths: 20 16 16 16 60
    :header-rows: 1

    * - Option
      - Type
      - Default
      - Required
      - Description
    * - --address TEXT
      - str
      - None
      - NO
      - Specifies the Ray cluster address. If not provided, the **RAY_ADDRESS** environment variable is used.
    * - <virtual-cluster-id>
      - str
      - N/A
      - YES
      - The unique identifier of the Virtual Cluster to be removed.

**Usage Example:**

**Example 1:** Removing a Virtual Cluster by ID

`ray vcluster remove logical1`

**Output:**

`Virtual cluster 'logical1' removed successfully`

**Example 2:** Handling Removal Failure Due to Non-Existent ID

`ray vcluster remove unknownCluster`

**Output:**

`Failed to remove virtual cluster 'unknownCluster': The logical cluster unknownCluster does not exist.`

List Virtual Clusters
---------------------

**Description:**

Displays a summary of all Virtual Clusters in your Ray environment. By default, it presents a table listing each cluster's ID, divisibility status, and any subdivided clusters. The `--detail` flag enriches the output with comprehensive information, including replica distributions and node instance statuses. The `--format` option allows output customization in `default`, `json`, `yaml`, or `table` formats.

**Command:**

`ray list vclusters [OPTIONS]`

**Options:**

.. list-table::
    :widths: 20 16 60
    :header-rows: 1

    * - Option
      - Type
      - Description
    * - --format <format>
      - str
      - Specify the output format: `default`, `json`, `yaml`, or `table`.
    * - -f, --filter TEXT
      - str
      - Apply filter expressions to narrow down the list based on specific criteria. Multiple filters are combined using logical AND.
    * - --limit INTEGER
      - int
      - Maximum number of entries to return (default: `100`).
    * - --detail
      - bool
      - Include detailed information in the output.
    * - --timeout INTEGER
      - int
      - Timeout in seconds for the API requests (default: `30`).
    * - --address TEXT
      - str
      - Address of the Ray API server. If not provided, it is configured automatically.

**Sample Output:**

- Brief outputs:

.. code-block:: text

    $ ray list vclusters

    ======== List: 2025-01-20 16:50:30.665928 ========
    Stats:
    ------------------------------
    Total: 4
    
    Table:
    ------------------------------
        VIRTUAL_CLUSTER_ID       DIVISIBLE    DIVIDED_CLUSTERS                      REPLICA_SETS    UNDIVIDED_REPLICA_SETS    RESOURCES_USAGE
     0  kPrimaryClusterID        True         kPrimaryClusterID##job1: indivisible  group0: 2       group0: 1                 CPU: 2.0 / 41.0
                                              logical1: divisble                    group1: 1       group1: 1                 memory: 2.000 GiB / 68.931 GiB
                                                                                    group2: 2                                 object_store_memory: 0.000 B / 23.793 GiB
     1  kPrimaryClusterID##job1  False        {}                                    group0: 1       group0: 1                 CPU: 1.0 / 9.0
                                                                                                                              memory: 1.000 GiB / 9.327 GiB
                                                                                                                              object_store_memory: 0.000 B / 4.663 GiB
     2  logical1                 True         logical1##job2: indivisible           group2: 2       group2: 1                 CPU: 1.0 / 16.0
                                                                                                                              memory: 1.000 GiB / 29.802 GiB
                                                                                                                              object_store_memory: 0.000 B / 9.565 GiB
     3  logical1##job2           False        {}                                    group2: 1       group2: 1                 CPU: 1.0 / 8.0
                                                                                                                              memory: 1.000 GiB / 14.901 GiB
                                                                                                                              object_store_memory: 0.000 B / 4.783 GiB

- Detailed outputs:

.. code-block:: yaml

   $ ray list vclusters --detail

   ---
   -   virtual_cluster_id: kPrimaryClusterID
       divisible: true
       divided_clusters:
           logical1: divisble
           kPrimaryClusterID##job1: indivisible
       replica_sets:
           group0: 2
           group1: 1
           group2: 2
       undivided_replica_sets:
           group1: 1
           group0: 1
       resources_usage:
           CPU: 2.0 / 41.0
           object_store_memory: 0.000 B / 23.793 GiB
           memory: 2.000 GiB / 68.931 GiB
       visible_node_instances:
           fe8e2961e1d7f72c8f9da7bea38ebb650cbee685f541e8ceedb2a8e3:
               hostname: arconkube-40-100083029097
               template_id: group1
               is_dead: false
           740273507b09c082c33909e9134ce136d1743e0da1d5b68ec2574988:
               hostname: arconkube-40-100083029138
               template_id: group0
               is_dead: false
           3505335a78b9955a1c2ed1de0a0fa92449b8011afddb621b2bab23d5:
               hostname: arconkube-40-100083029093
               template_id: group0
               is_dead: false
       undivided_nodes:
           fe8e2961e1d7f72c8f9da7bea38ebb650cbee685f541e8ceedb2a8e3:
               hostname: arconkube-40-100083029097
               template_id: group1
               is_dead: false
           740273507b09c082c33909e9134ce136d1743e0da1d5b68ec2574988:
               hostname: arconkube-40-100083029138
               template_id: group0
               is_dead: false

**Explanation:**

- **Primary Cluster** (`kPrimaryClusterID`):

  - **Divisible:** `true` - can create sub-clusters.
  - **Divided Clusters:** Includes `kPrimaryClusterID##job1` and `logical1`.
  - **Replica Sets:** Distribution across `group2`, `group1`, and `group0`.
  - **Visible Node Instances:** Lists active nodes with their details.
  - **Undivided Nodes:** Empty, as all nodes are part of sub-clusters.

- **Logical Cluster** (`logical1`):

  - **Divisible:** `true` - can be subdivided.
  - **Replica Sets & Undivided Replica Sets:** Reflects replica distribution.
  - **Visible Node Instances & Undivided Nodes:** Lists nodes associated exclusively with this logical cluster.

**Filtering Options:**

The `--filter` flag enables you to narrow down the list of Virtual Clusters based on specific attributes. Multiple `--filter` options can be specified, and they are concatenated using logical AND. Filter expressions support predicates such as `key=value` or `key!=value`, and string filter values are case-insensitive.

- **Supported Filter Expressions**

  - **Divisibility:**
  
    - `divisible=true`: Lists only divisible clusters.
    - `divisible=false`: Lists only indivisible clusters.
  
  - **Virtual Cluster ID:**

    - `virtual_cluster_id=vid1`: Retrieves information for the cluster with ID `vid1`.

- **Usage Guidelines**

  - **Single Filter:** `ray list vclusters --detail --filter "divisible=true"`
  - **Multiple Filters:** `ray list vclusters --detail --filter "divisible=true" --filter "virtual_cluster_id=kPrimaryClusterID"`

  **Note:** Combining multiple filters results in a logical AND operation, meaning only clusters that satisfy all filter conditions will be listed.

Get Specific Virtual Cluster
----------------------------

**Description:**

Fetches detailed information about a single Virtual Cluster identified by its `virtual_cluster_id`.

**Command:**

`ray get vclusters <virtual_cluster_id> [OPTIONS]`

**Options:**

.. list-table::
    :widths: 20 16 60
    :header-rows: 1

    * - Option
      - Type
      - Description
    * - --format <format>
      - str
      - Specify the output format: `default`, `json`, `yaml`, or `table`.
    * - --timeout INTEGER
      - int
      - Timeout in seconds for the API requests (default: `30`).
    * - --address TEXT
      - str
      - Address of the Ray API server. If not provided, it is configured automatically.

Understanding Command Outputs
-----------------------------

Each Virtual Cluster's information comprises several key fields:

**Common Fields**

- virtual_cluster_id:

  - A unique identifier for the Virtual Cluster. IDs may include suffixes (e.g., ##job1, ##logical1) indicating Job Clusters with specific job IDs or Logical Clusters.

- divisible:

  - Indicates whether the cluster is Divisible (true) or Indivisible (false).
    
    - Divisible Cluster (true): Can be subdivided into Logical Clusters or Job Clusters.
    - Indivisible Cluster (false): Cannot be subdivided and is used exclusively for hosting user-submitted jobs.

- divided_clusters:

  - Lists sub-clusters that have been subdivided from the parent cluster. This field is empty for Indivisible Clusters.

- replica_sets:

  - Details the distribution of replicas across different template groups within the cluster, excluding any inactive nodes.

- undivided_replica_sets:

  - Similar to replica_sets but specifically for replicas not associated with any sub-cluster.

- visible_node_instances:

  - A dictionary of visible node instances within the cluster, including:

    - Node ID: Unique identifier for each node.
    - hostname: Network name of the node.
    - template_id: Indicates the template group the node belongs to (e.g., group2).
    - is_dead: Boolean flag indicating node status (false for active, true for inactive or failed).

- undivided_nodes:

  - Visible nodes that are part of the cluster but not associated with any divided sub-cluster.
