def teardown_cluster(config_file: str, yes: bool, workers_only: bool,
                     override_cluster_name: Optional[str],
                     keep_min_workers: bool):
    """Destroys all nodes of a Ray cluster described by a config json."""
    config = yaml.safe_load(open(config_file).read())
    if override_cluster_name is not None:
        config["cluster_name"] = override_cluster_name
    config = prepare_config(config)
    validate_config(config)

    cli_logger.confirm(yes, "Destroying cluster.", _abort=True)
    cli_logger.old_confirm("This will destroy your cluster", yes)

    if not workers_only:
        try:
            exec_cluster(
                config_file,
                cmd="ray stop",
                run_env="auto",
                screen=False,
                tmux=False,
                stop=False,
                start=False,
                override_cluster_name=override_cluster_name,
                port_forward=None,
                with_output=False)
        except Exception as e:
            # todo: add better exception info
            cli_logger.verbose_error("{}", str(e))
            cli_logger.warning(
                "Exception occurred when stopping the cluster Ray runtime "
                "(use -v to dump teardown exceptions).")
            cli_logger.warning(
                "Ignoring the exception and "
                "attempting to shut down the cluster nodes anyway.")

            cli_logger.old_exception(
                logger, "Ignoring error attempting a clean shutdown.")

    provider = _get_node_provider(config["provider"], config["cluster_name"])
    try:

        def remaining_nodes():
            workers = provider.non_terminated_nodes({
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER
            })

            if keep_min_workers:
                min_workers = config.get("min_workers", 0)

                cli_logger.print(
                    "{} random worker nodes will not be shut down. " +
                    cf.dimmed("(due to {})"), cf.bold(min_workers),
                    cf.bold("--keep-min-workers"))
                cli_logger.old_info(logger,
                                    "teardown_cluster: Keeping {} nodes...",
                                    min_workers)

                workers = random.sample(workers, len(workers) - min_workers)

            # todo: it's weird to kill the head node but not all workers
            if workers_only:
                cli_logger.print(
                    "The head node will not be shut down. " +
                    cf.dimmed("(due to {})"), cf.bold("--workers-only"))

                return workers

            head = provider.non_terminated_nodes({
                TAG_RAY_NODE_KIND: NODE_KIND_HEAD
            })

            return head + workers

        def run_docker_stop(node, container_name):
            try:
                exec_cluster(
                    config_file,
                    cmd=f"docker stop {container_name}",
                    run_env="host",
                    screen=False,
                    tmux=False,
                    stop=False,
                    start=False,
                    override_cluster_name=override_cluster_name,
                    port_forward=None,
                    with_output=False)
            except Exception:
                cli_logger.warning(f"Docker stop failed on {node}")
                cli_logger.old_warning(logger, f"Docker stop failed on {node}")

        # Loop here to check that both the head and worker nodes are actually
        #   really gone
        A = remaining_nodes()

        container_name = config.get("docker", {}).get("container_name")
        if container_name:
            for node in A:
                run_docker_stop(node, container_name)

        with LogTimer("teardown_cluster: done."):
            while A:
                cli_logger.old_info(
                    logger, "teardown_cluster: "
                    "Shutting down {} nodes...", len(A))

                provider.terminate_nodes(A)

                cli_logger.print(
                    "Requested {} nodes to shut down.",
                    cf.bold(len(A)),
                    _tags=dict(interval="1s"))

                time.sleep(
                    POLL_INTERVAL)  # todo: interval should be a variable
                A = remaining_nodes()
                cli_logger.print("{} nodes remaining after {} second(s).",
                                 cf.bold(len(A)), POLL_INTERVAL)
            cli_logger.success("No nodes remaining.")
    finally:
        provider.cleanup()