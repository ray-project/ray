
# Exploration as callbacks. 

This is mostly TBD. But the current Exploration modules are some sort of callbacks. 
We can unify them with the existing Callbacks and expand the hooks for callbacks. 
Now, callback objects can get passed into UnitTrainer and even RLModule to have different hooks for different things. 
For this we should converge on the API for UnitTrainers and RLModules and all the weird 
cases that could happen and need to be naturally supported without any hacks. 

In principle all Exploration methods, both the unsupervised RL ones (e.g. curiosity) 
and action modification ones (e.g. Epsilon greedy)
can be implemented this way. The learning based ones, will just add an extra optimizer 
and loss term that can be used to sum together the loss of policy and the curiosity 
loss to update them both using one optimizer.

We will run the callbacks in the order specified for all hooks by default. 
But user-defined orders can be supported for each hook too.

```python
class RLlibCallbacks:
    
    ...
    
    # for supporting exploration
    def on_postprocess_trajectory(self, ...):
        """
        This can be used for adding intrinsic reward to the extrinsic reward.
        """
        pass

    def on_after_forward(self, ...):
        """
        This can be used for modifying the output distribution of the policy for sampling actions from the env.
        # can implement epsilon greedy
        """
        pass

    def on_episode_start(self, ...):
        """
        This can be used to add noise to the RLModule parameters that samples actions
        """
        pass
    
    def on_episode_end(self, ...):
        pass
    
    def on_after_loss(self, ...):
        """
        This can augment the loss dict returned by the UnitTrainer.loss() to augment the loss with specific losses from Exploration
        """
        pass
        
    def on_before_loss(self, ...):
        """
        Preparation before calling UnitTrainer.loss() 
        """
        pass
    
    def on_make_optimizer(self, ...):
        """
        After calling make_optimizer inside the init of the UnitTrainer
        """
        pass
    

```
