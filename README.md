![](https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png)

[![](https://readthedocs.org/projects/ray/badge/?version=master)](https://docs.ray.io/en/master/?badge=master)

[![](https://img.shields.io/badge/Ray-Join%20Slack-blue)](https://www.ray.io/join-slack)

[![](https://img.shields.io/badge/Discuss-Ask%20Questions-blue)](https://discuss.ray.io/)

[![](https://img.shields.io/twitter/follow/raydistributed.svg?style=social&logo=twitter)](https://x.com/raydistributed)

[![](https://img.shields.io/badge/Get_started_for_free-3C8AE9?logo=data%3Aimage%2Fpng%3Bbase64%2CiVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8%2F9hAAAAAXNSR0IArs4c6QAAAERlWElmTU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAA6ABAAMAAAABAAEAAKACAAQAAAABAAAAEKADAAQAAAABAAAAEAAAAAA0VXHyAAABKElEQVQ4Ea2TvWoCQRRGnWCVWChIIlikC9hpJdikSbGgaONbpAoY8gKBdAGfwkfwKQypLQ1sEGyMYhN1Pd%2B6A8PqwBZeOHt%2FvsvMnd3ZXBRFPQjBZ9K6OY8ZxF%2B0IYw9PW3qz8aY6lk92bZ%2BVqSI3oC9T7%2FyCVnrF1ngj93us%2B540sf5BrCDfw9b6jJ5lx%2FyjtGKBBXc3cnqx0INN4ImbI%2Bl%2BPnI8zWfFEr4chLLrWHCp9OO9j19Kbc91HX0zzzBO8EbLK2Iv4ZvNO3is3h6jb%2BCwO0iL8AaWqB7ILPTxq3kDypqvBuYuwswqo6wgYJbT8XxBPZ8KS1TepkFdC79TAHHce%2F7LbVioi3wEfTpmeKtPRGEeoldSP%2FOeoEftpP4BRbgXrYZefsAI%2BP9JU7ImyEAAAAASUVORK5CYII%3D)](https://www.anyscale.com/ray-on-anyscale?utm_source=github&utm_medium=ray_readme&utm_campaign=get_started_badge)

Ray is a unified framework for scaling AI and Python applications. Ray consists of a core distributed runtime and a set of AI libraries for simplifying ML compute:

![](https://github.com/ray-project/ray/raw/master/doc/source/images/what-is-ray-padded.svg)

<!-- https://docs.google.com/drawings/d/1Pl8aCYOsZCo61cmp57c7Sja6HhIygGCvSZLi_AuBuqo/edit -->

Learn more about [Ray AI Libraries](https://docs.ray.io/en/latest/ray-air/getting-started.html):

- [Data](https://docs.ray.io/en/latest/data/data.html): Scalable Datasets for ML
- [Train](https://docs.ray.io/en/latest/train/train.html): Distributed Training
- [Tune](https://docs.ray.io/en/latest/tune/index.html): Scalable Hyperparameter Tuning
- [RLlib](https://docs.ray.io/en/latest/rllib/index.html): Scalable Reinforcement Learning
- [Serve](https://docs.ray.io/en/latest/serve/index.html): Scalable and Programmable Serving

Or more about [Ray Core](https://docs.ray.io/en/latest/ray-core/walkthrough.html) and its key abstractions:

- [Tasks](https://docs.ray.io/en/latest/ray-core/tasks.html): Stateless functions executed in the cluster.
- [Actors](https://docs.ray.io/en/latest/ray-core/actors.html): Stateful worker processes created in the cluster.
- [Objects](https://docs.ray.io/en/latest/ray-core/objects.html): Immutable values accessible across the cluster.

Learn more about Monitoring and Debugging:

- Monitor Ray apps and clusters with the [Ray Dashboard](https://docs.ray.io/en/latest/ray-core/ray-dashboard.html).
- Debug Ray apps with the [Ray Distributed Debugger](https://docs.ray.io/en/latest/ray-observability/ray-distributed-debugger.html).

Ray runs on any machine, cluster, cloud provider, and Kubernetes, and features a growing
[ecosystem of community integrations](https://docs.ray.io/en/latest/ray-overview/ray-libraries.html).

Install Ray with: `pip install ray`. For nightly wheels, see the
[Installation page](https://docs.ray.io/en/latest/ray-overview/installation.html).

## Why Ray?

Today's ML workloads are increasingly compute-intensive. As convenient as they are, single-node development environments such as your laptop cannot scale to meet these demands.

Ray is a unified way to scale Python and AI applications from a laptop to a cluster.

With Ray, you can seamlessly scale the same code from a laptop to a cluster. Ray is designed to be general-purpose, meaning that it can performantly run any kind of workload. If your application is written in Python, you can scale it with Ray, no other infrastructure required.

## More Information

- [Documentation](https://docs.ray.io/en/latest/index.html)
- [Ray Architecture whitepaper](https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview)
- [Exoshuffle: large-scale data shuffle in Ray](https://arxiv.org/abs/2203.05072)
- [Ownership: a distributed futures system for fine-grained tasks](https://www.usenix.org/system/files/nsdi21-wang.pdf)
- [RLlib paper](https://arxiv.org/abs/1712.09381)
- [Tune paper](https://arxiv.org/abs/1807.05118)

*Older documents:*

- [Ray paper](https://arxiv.org/abs/1712.05889)
- [Ray HotOS paper](https://arxiv.org/abs/1703.03924)
- [Ray Architecture v1 whitepaper](https://docs.google.com/document/d/1lAy0Owi-vPz2jEqBSaHNQcy2IBSDEHyXNOQZlGuj93c/preview)

## Getting Involved

| Platform | Purpose | Estimated Response Time | Support Level |
|---|---|---|---|
| [Discourse Forum](https://discuss.ray.io/) | For discussions about development and questions about usage. | < 1 day | Community |
| [GitHub Issues](https://github.com/ray-project/ray/issues) | For reporting bugs and filing feature requests. | < 2 days | Ray OSS Team |
| [Slack](https://www.ray.io/join-slack?utm_source=github&utm_medium=ray_readme&utm_campaign=getting_involved) | For collaborating with other Ray users. | < 2 days | Community |
| [StackOverflow](https://stackoverflow.com/questions/tagged/ray) | For asking questions about how to use Ray. | 3-5 days | Community |
| [Meetup Group](https://www.meetup.com/Bay-Area-Ray-Meetup/) | For learning about Ray projects and best practices. | Monthly | Ray DevRel |
| [Twitter](https://x.com/raydistributed) | For staying up-to-date on new features. | Daily | Ray DevRel |
