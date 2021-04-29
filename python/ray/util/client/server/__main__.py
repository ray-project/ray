if __name__ == "__main__":
    from ray.util.client.server.server import main
    try:
        import anyscale
        anyscale.utils.runtime_env.runtime_env_setup()
    except:
        print("Failed to run setup")
    main()
