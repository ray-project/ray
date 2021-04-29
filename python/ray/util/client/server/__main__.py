if __name__ == "__main__":
    from ray.util.client.server.server import main
    try:
        from anyscale.utils.runtime_env import runtime_env_setup
        runtime_env_setup()
    except:
        print("Failed to run setup")
    main()
