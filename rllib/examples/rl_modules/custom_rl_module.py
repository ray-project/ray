"""Example of implementing and configuring a custom (torch) RLModule.

This example:
    - demonstrates how you can subclass the TorchRLModule base class and setup your
    own neural network architecture by overriding `setup()`.
    - how to override the 3 forward methods: `_forward_inference`, `_forward_exploration`,
    and `forward_train` to implement your own custom forward logic(s). You will also learn,
    when each of these 3 methods is called by RLlib or the users of your RLModule.
    - shows how you then configure an RLlib Algorithm such that it uses your custom
    RLModule (instead of a default RLModule).

We implement a tiny CNN stack here, the exact same one that is used by the old API
stack as default CNN net. It comprises 4 convolutional layers, the last of which
ends in a 1x1 filter size and the number of filters exactly matches the number of
discrete actions (logits). This way, the (non-activated) output of the last layer only
needs to be reshaped in order to receive the policy's logit outputs. No flattening
or additional dense layer required.

The network is then used in a fast ALE/Pong-v5 experiment.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should see the following output (at the end of the experiment) in your console:

"""



