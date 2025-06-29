Production Deployments
======================

In this section, we'll discuss some key considerations for running our workloads in production. `Anyscale <https://console.anyscale.com/register/ha?render_flow=ray&utm_source=ray_docs&utm_medium=docs&utm_campaign=tutorial>`_ offers the fastest path to production, and we'll use it to deploy our application following best practices.

For teams that do not want to use Anyscale, KubeRay offers an alternative way to deploy Ray applications to production. However, KubeRay requires deep Kubernetes knowledge and a significant amount of setup, configuration, and hardening to be production-ready, and so this will not be covered in this tutorial.

Computing Embeddings
--------------------

TODO: add instructions

Training the Classifier
-----------------------

TODO: add instructions

Deploying the Search Application
--------------------------------

    Now that we have validated the service, we can deploy it as an Anyscale Service. Anyscale Services are a managed service for deploying and scaling production-readyRay Serve applications. Anyscale Services automatically set up the load balancer, highly-availability, observability, and authentication, and offer canary deployments and rollbacks out-of-the-box.

    To deploy a production service, the best practice is to define a service configuration file that specifies the service name, the Ray Serve application, and the Ray Serve deployment configuration. We will use the `doggos_service.yaml` file to deploy the service:

    .. literalinclude:: ../../examples/e2e-multimodal-ai-workloads/doggos_service.yaml

    Use the Anyscale CLI plus the to deploy the Anyscale Service:

    .. code-block:: bash

        anyscale service deploy --name doggos-service --config doggos_service.yaml

    Once the service has been deployed, you can test it by running the following command in a new terminal:

    .. code-block:: bash

        curl -X POST https://doggos-service.anyscale.com/classify -H "Content-Type: application/json" -d '{"image_url": "https://example.com/image.jpg"}'

    To learn more about Anyscale Services, see the `Anyscale documentation <https://docs.anyscale.com/platform/services/>`_.

Best Practices for Production
-----------------------------

When deploying to production, keep these best practices in mind:

1. **Monitoring and Logging**
   - Set up comprehensive monitoring
   - Log all important events
   - Set up alerts for critical issues

2. **Error Handling**
   - Implement robust error handling
   - Set up automatic retries
   - Handle edge cases gracefully

3. **Security**
   - Secure your endpoints
   - Implement rate limiting
   - Use proper authentication

4. **Scaling**
   - Monitor resource usage
   - Set up auto-scaling
   - Handle traffic spikes

5. **Testing**
   - Write comprehensive tests
   - Test in staging environment
   - Perform load testing

Conclusion
----------

Congratulations! You've now learned how to:

1. Process data at scale with Ray Data
2. Train models distributedly with Ray Train
3. Serve models with Ray Serve
4. Deploy to production with Ray Jobs and Services

You can now use these skills to build and deploy your own scalable AI applications with Ray!

Next Steps
----------

To continue learning about Ray, check out:

1. :doc:`/ray-overview/index` - Ray's core concepts
2. :doc:`/ray-core/walkthrough` - Ray Core walkthrough
3. :doc:`/data/data` - Ray Data walkthrough
4. :doc:`/train/train` - Ray Train walkthrough
5. :doc:`/serve/index` - Ray Serve walkthrough

For more information about each component, check out:
1. :doc:`/ray-overview/index` - Ray's core concepts
2. :doc:`/ray-core/walkthrough` - Ray Core walkthrough
3. :doc:`/data/data` - Ray Data walkthrough
4. :doc:`/train/train` - Ray Train walkthrough
5. :doc:`/serve/index` - Ray Serve walkthrough 