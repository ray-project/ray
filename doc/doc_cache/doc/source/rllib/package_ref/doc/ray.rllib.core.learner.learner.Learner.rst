

ray.rllib.core.learner.learner.Learner
======================================

.. currentmodule:: ray.rllib.core.learner.learner

.. autoclass:: Learner
   :show-inheritance:

   
   
   .. rubric:: Methods

   .. autosummary::
      :nosignatures:
      :toctree:

   
      
      ~Learner.add_module
      ~Learner.additional_update
      ~Learner.additional_update_for_module
      
      ~Learner.apply_gradients
      ~Learner.build
      
      ~Learner.compute_gradients
      ~Learner.compute_loss
      ~Learner.compute_loss_for_module
      ~Learner.configure_optimizers
      ~Learner.configure_optimizers_for_module
      ~Learner.filter_param_dict_for_optimizer
      ~Learner.get_module_state
      ~Learner.get_optimizer
      ~Learner.get_optimizer_state
      ~Learner.get_optimizers_for_module
      ~Learner.get_param_ref
      ~Learner.get_parameters
      ~Learner.get_state
      ~Learner.load_state
      ~Learner.postprocess_gradients
      ~Learner.postprocess_gradients_for_module
      
      
      ~Learner.register_optimizer
      ~Learner.remove_module
      ~Learner.save_state
      ~Learner.set_module_state
      ~Learner.set_optimizer_state
      ~Learner.set_state
      ~Learner.should_module_be_updated
      ~Learner.update_from_batch
      ~Learner.update_from_episodes

   
   


   
   
   .. rubric:: Attributes

   .. autosummary::
      :nosignatures:
      :toctree:

   
      ~Learner.TOTAL_LOSS_KEY
      ~Learner.distributed
      ~Learner.framework
      ~Learner.module

   
   