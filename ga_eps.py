# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import namedtuple
import logging
import numpy as np
import time

import ray
from ray.rllib.agents import Agent, with_common_config

from ray.rllib.agents.ga_eps import optimizers
from ray.rllib.agents.ga_eps import policies
from ray.rllib.agents.ga_eps import utils
from ray.rllib.agents.ga_eps import tf_util
from ray.rllib.utils import FilterManager

import gym
import tensorflow as tf
from ray.rllib.env.atari_wrappers import is_atari, get_wrapper_by_cls, MonitorEnv, EpisodicLifeEnv, wrap_deepmind


logger = logging.getLogger(__name__)

Result = namedtuple("Result", [
    "noise_indices", "noisy_returns", "noisy_acc_returns","sign_noisy_returns", "noisy_lengths",
    "eval_returns", "eval_acc_returns","eval_lengths","novelty","policy_weights"])

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    "l2_coeff": 0.005,
    "noise_stdev": 0.02,
    "noise_stdevGA": 0.02,
    "episodes_per_batch": 7,
    "train_batch_size": 1000,
    "eval_prob": 0.003,
    "return_proc_mode": "centered_rank",
    "num_workers": 7,#10
    "stepsize": 0.01,
    "observation_filter": "MeanStdFilter",
    "noise_size": 250000000,
    "report_length": 10,
    "pop_size": 1,
    "population_size":20,
    "bf_sz":2000,
    "k":10,
    "epsilon":0.5,
    "neinum":10,
    "eta_c":20
})
# __sphinx_doc_end__
# yapf: enable


@ray.remote
def create_shared_noise(count):
    """Create a large array of noise to be shared by all workers."""
    seed = 123
    noise = np.random.RandomState(seed).randn(count).astype(np.float32)
    return noise


class SharedNoiseTable(object):
    def __init__(self, noise):
        self.noise = noise
        assert self.noise.dtype == np.float32

    def get(self, i, dim):
        return self.noise[i:i + dim]

    def sample_index(self, dim):
        return np.random.randint(0, len(self.noise) - dim + 1)

# def get_ref_batch(env, batch_size):
#     ref_batch = []
#     observation = env.reset()
#     while len(ref_batch) < batch_size:
#         observation, rew, done, info= env.step(env.action_space.sample())
#         ref_batch.append(observation)
#         if done:
#             observation = env.reset()
#     return ref_batch

def _monitor(env, path):
    return gym.wrappers.Monitor(env, path, resume=True)




@ray.remote
class Worker(object):
    def __init__(self,
                 config,
                 policy_params,
                 env_creator,
                 noise,
                 monitor_path,
                 min_task_runtime=0.2):
        self.min_task_runtime = min_task_runtime
        self.config = config
        self.policy_params = policy_params
        self.noise = SharedNoiseTable(noise)
        
        self.monitor_path = monitor_path
        self.env = env_creator(config["env_config"])
        self.memory=[]

        if is_atari(self.env):
            self.env = wrap_deepmind(
                    self.env,
                    dim=84,
                    framestack=4)

        if np.random.uniform()<0.5:
          if self.monitor_path:
             self.env = _monitor(self.env, self.monitor_path)
        else:
           self.env = env_creator(config["env_config"]) 
           if is_atari(self.env):
                self.env = wrap_deepmind(
                    self.env,
                    dim=84,
                    framestack=4)

        self.env2=env_creator(config["env_config"])
        if is_atari(self.env2):
                self.env2 = wrap_deepmind(
                    self.env2,
                    dim=84,
                    framestack=4)

        from ray.rllib import models
        self.preprocessor = models.ModelCatalog.get_preprocessor(
            self.env, config["model"])

        self.sess = utils.make_session(single_threaded=True)

        self.policy = policies.GenericPolicy(
            self.sess, self.env,self.env2,self.env.action_space, self.env.observation_space,
            self.preprocessor, config["observation_filter"], config["model"],
            **policy_params)
        #self.sess.run(tf.global_variables_initializer())


        #self.sess1 = utils.make_session(single_threaded=True)

        # self.policymax = policies.GenericPolicy(
        #     self.sess, self.env,self.env2,self.env.action_space, self.env.observation_space,
        #     self.preprocessor, config["observation_filter"], config["model"],
        #     **policy_params)
        self.sess.run(tf.global_variables_initializer())
        

    @property
    def filters(self):
        return {"default": self.policy.get_filter()}

    def sync_filters(self, new_filters):
        for k in self.filters:
            self.filters[k].sync(new_filters[k])

    def get_filters(self, flush_after=False):
        return_filters = {}
        for k, f in self.filters.items():
            return_filters[k] = f.as_serializable()
            if flush_after:
                f.clear_buffer()
        return return_filters

    def rollout(self, timestep_limit, add_noise=True):

        novelty,rollout_returns,rollout_rewards, rollout_length= policies.rollout(
            self.policy,
            self.config,
            self.env,
            timestep_limit=timestep_limit,
            add_noise=add_noise)
        return novelty,rollout_returns,rollout_rewards, rollout_length

    def do_rollouts(self, params, population,timestep_limit=None):

        # Set the network weights.
        self.policy.set_weights(params)

        noise_indices, acc_returns,returns, sign_returns, lengths,novelty,rew_chgs,entro_chgs,distances = [], [], [], [], [],[],[],[],[]
        eval_acc_returns,eval_returns, eval_lengths = [], [], []
        policy_weights=[]

        # Perform some rollouts with noise.
        task_tstart = time.time()
        while (len(noise_indices) == 0
               or time.time() - task_tstart < self.min_task_runtime):

            if np.random.uniform() < self.config["eval_prob"]:
                # Do an evaluation run with no perturbation.
                #print("enter <")
                self.policy.set_weights(params)
                nov,acc_rewards,rewards, length= self.rollout(timestep_limit,add_noise=False)
                eval_acc_returns.append(acc_rewards.sum())
                eval_returns.append(rewards.sum())
                eval_lengths.append(length)
       
            else:
                noise_index = self.noise.sample_index(self.policy.num_params)

                perturbation = self.config["noise_stdevGA"] * self.noise.get(
                    noise_index, self.policy.num_params)
                
                if len(population)==0:
                	v=self.noise.get(noise_index,self.policy.num_params)
                else:
                    v1=population[np.random.randint(len(population))][0]
                    v2=population[np.random.randint(len(population))][0]

                    u=np.random.rand(1)
                    u=u[0]
                    if u<=0.5:
                    	beta=(2*u)**(1.0/(self.config["eta_c"]+1))
                    else:
                    	beta=(1.0/(2*(1-u)))**(1.0/(self.config["eta_c"]+1))
                    #print('v1 is {}'.format(v1))
                    v1=np.array(v1)
                    v2=np.array(v2)
                    v=0.5*((1+beta)*v1+(1-beta)*v2)
                    v=v.tolist()
                    #v=population[0][0]

                v=v+perturbation

                self.policy.set_weights(v)

                nov_pos,acc_rewards_pos,rewards_pos, lengths_pos= self.rollout(timestep_limit)
                policy_pos=self.policy.get_weights()

                noise_indices.append(noise_index)
                returns.append(rewards_pos.sum())
                acc_returns.append(acc_rewards_pos.sum())
                sign_returns.append(
                    [np.sign(rewards_pos).sum()])
                lengths.append([lengths_pos])
                novelty.append([nov_pos[0]])
                policy_weights.append([policy_pos])

                #cross_entros=np.array(cross_entros,dtype=np.float32)




        return Result(
            noise_indices=noise_indices,
            noisy_returns=returns,
            noisy_acc_returns=acc_returns,
            sign_noisy_returns=sign_returns,
            noisy_lengths=lengths,
            eval_returns=eval_returns,
            eval_acc_returns=eval_acc_returns,
            eval_lengths=eval_lengths,
            novelty=novelty,
            policy_weights=policy_weights)


class ESAgent(Agent):
    """Large-scale implementation of Evolution Strategies in Ray."""

    _agent_name = "GA_eps"
    _default_config = DEFAULT_CONFIG

    def _init(self):
        policy_params = {"action_noise_std": 0.01}
        self.theta_dict = []
        self.policymax=[]
        self.curr_parent = 0
        self.population=[]
        self.returns_n2=[]
        self.ret=[]

        env = self.env_creator(self.config["env_config"])
        self.monitor_path=self.logdir if self.config["monitor"] else None
        if is_atari(env):
            env = wrap_deepmind(
                    env,
                    dim=84,
                    framestack=4)

        env2=self.env_creator(self.config["env_config"])
        if is_atari(env2):
                env2 = wrap_deepmind(
                    env2,
                    dim=84,
                    framestack=4)

        from ray.rllib import models
        preprocessor = models.ModelCatalog.get_preprocessor(env)       
        
        for p in range(self.config["pop_size"]):
            with tf.Graph().as_default():
                self.sess = utils.make_session(single_threaded=False)
                self.policy = policies.GenericPolicy(self.sess, env,env2,env.action_space, env.observation_space, preprocessor, self.config["observation_filter"], self.config["model"],**policy_params) 
                tf_util.initialize(self.sess)
                theta= self.policy.get_weights()
                self.theta_dict.append(theta)





            
        
        self.optimizer = optimizers.Adam(self.policy, self.config["stepsize"])
        self.report_length = self.config["report_length"]
        # Create the shared noise table.
        logger.info("Creating shared noise table.")
        noise_id = create_shared_noise.remote(self.config["noise_size"])
        self.noise = SharedNoiseTable(ray.get(noise_id))

        # for p in range(self.config["population_size"]): 
        #     noise_index = self.noise.sample_index(self.policy.num_params)     	
        #     self.population.append(self.noise.get(noise_index, self.policy.num_params))

        # Create the actors.
        logger.info("Creating actors.")
        self.workers = [
            Worker.remote(self.config, policy_params, self.env_creator,
                          noise_id, self.monitor_path) for _ in range(self.config["num_workers"])
        ]

        self.episodes_so_far = 0
        self.reward_list1 = []
        self.reward_list2 = []
        self.reward_list3 = []
        self.tstart = time.time()

        self.noisy_rew_max1=-1000
        self.noisy_rew_mean1=-1000
        self.noisy_rew_max2=-1000
        self.noisy_rew_mean2=-1000
        self.noisy_rew_max3=-1000
        self.noisy_rew_mean3=-1000
        self.noisy_rew_max4=-1000
        self.noisy_rew_mean4=-1000

        self.reward_mean1=-1000
        self.reward_mean2=-1000
        self.reward_mean3=-1000

        self.maxrew=[]
        self.maxrew2=[]

        

    def _collect_results(self, theta_id, min_episodes, min_timesteps):
        num_episodes, num_timesteps = 0, 0
        reward_mean,reward_max,reward_min=0,0,0
        results = []
        r1=[]
        r2=[]
        policy_candidate1=[]
        if self.curr_parent==0:
            rewmax=self.noisy_rew_max1
            rewmean=self.noisy_rew_mean1
            self.policymax=[]
            self.maxrew=[]
            self.maxrew2=[]          
        elif self.curr_parent==1:
        	rewmax=self.noisy_rew_max2
        	rewmean=self.noisy_rew_mean2
        elif self.curr_parent==2:
        	rewmax=self.noisy_rew_max3
        	rewmean=self.noisy_rew_mean3
        elif self.curr_parent==3:
        	rewmax=self.noisy_rew_max4
        	rewmean=self.noisy_rew_mean4

        while num_episodes < min_episodes or num_timesteps < min_timesteps:
            logger.info(
                "Iteration {}, collected {} episodes {} timesteps so far this iter, cur_par={},rew_max={}, rew_mean={}, rew_min={}, last rew_max1={}, rew_mean1={},rew_max2={}, rew_mean2={},rew_max3={}, rew_mean3={},rew_max4={}, rew_mean4={}".format(
                    self.iteration+1,num_episodes, num_timesteps,self.curr_parent, reward_max,reward_mean,reward_min,self.noisy_rew_max1,self.noisy_rew_mean1,self.noisy_rew_max2,self.noisy_rew_mean2,self.noisy_rew_max3,self.noisy_rew_mean3,self.noisy_rew_max4,self.noisy_rew_mean4))
            rollout_ids = [
                worker.do_rollouts.remote(theta_id,self.population) for worker in self.workers
            ]
            # Get the results of the rollouts.
            #print('start')
            for result in ray.get(rollout_ids):
                results.append(result)
                r1+=result.noisy_returns
                r2+=result.noisy_acc_returns
                policy_candidate1+=result.policy_weights
                # if len(result.eval_returns)>0:
                #     r1+=result.eval_returns
                # Update the number of episodes and the number of timesteps
                # keeping in mind that result.noisy_lengths is a list of lists,
                # where the inner lists have length 2.
                num_episodes += sum(len(pair) for pair in result.noisy_lengths)
                num_timesteps += sum(
                    sum(pair) for pair in result.noisy_lengths)
            r=np.array(r1)
            r_acc=np.array(r2)
            #print('shape of r is {}'.format(r.shape))
            #print('policy_candidate1 is {}'.format(np.array(policy_candidate1).shape))
            reward_mean=np.mean(r)
            reward_max=np.max(r)
            reward_min=np.min(r)
        
        if self.curr_parent is not self.config["pop_size"]-1:
           r_acc_flat=r_acc.flatten()
           idx=np.argmax(r_acc_flat)
           policy_candidate=policy_candidate1[idx]

           
           self.policymax.append(policy_candidate)
           self.maxrew.append(r_acc_flat[idx])   #acc_return
           


           idx2=np.argmax(r.flatten())
           self.maxrew2.append(r.flatten()[idx2])   #return       

           	   # if self.maxrew[1]>self.maxrew[0]:
           	   # 	  temp=self.maxrew[0]
           	   # 	  temppolicy=self.policymax[0]
           	   # 	  self.maxrew[0]=self.maxrew[1]
           	   # 	  self.policymax[0]=self.policymax[1]
           	   # 	  self.maxrew[1]=temp
           	   # 	  self.policymax[1]=temppolicy
        

        if self.curr_parent==0:
        	self.noisy_rew_max1=reward_max
        	self.noisy_rew_mean1=reward_mean
        elif self.curr_parent==1:
        	self.noisy_rew_max2=reward_max
        	self.noisy_rew_mean2=reward_mean
        elif self.curr_parent==2:
        	self.noisy_rew_max3=reward_max
        	self.noisy_rew_mean3=reward_mean
        elif self.curr_parent==3:
        	self.noisy_rew_max4=reward_max
        	self.noisy_rew_mean4=reward_mean

        return results, num_episodes, num_timesteps,self.policymax,self.maxrew

    def _train(self):
        config = self.config
        
        theta = self.theta_dict[self.curr_parent]
        #print('theta shape is {}'.format(np.array(theta).shape))
        self.policy.set_weights(theta)
        assert theta.dtype == np.float32

        # Put the current policy weights in the object store.
        theta_id = ray.put(theta)
        # Use the actors to do rollouts, note that we pass in the ID of the
        # policy weights.
        results, num_episodes, num_timesteps, self.policymax, self.rewmax= self._collect_results(
            theta_id, config["episodes_per_batch"], config["train_batch_size"])

        all_noise_indices = []
        all_training_returns = []
        all_training_lengths = []
        all_eval_returns = []
        all_eval_lengths = []
        all_training_acc_returns=[]
        all_eval_acc_returns=[]
        all_policy_weight=[]
        all_novelty=[]
        all_rew_chgs=[]
        all_entro_chgs=[]
        all_distances=[]
     

        # Loop over the results.
        for result in results:
            all_eval_returns += result.eval_returns
            all_eval_lengths += result.eval_lengths
            all_eval_acc_returns+=result.eval_acc_returns


            all_noise_indices += result.noise_indices
            all_training_returns += result.noisy_returns
            all_training_lengths += result.noisy_lengths
            all_training_acc_returns+=result.noisy_acc_returns

            all_policy_weight+= result.policy_weights

            all_novelty+=result.novelty

        assert len(all_eval_returns) == len(all_eval_lengths)
        assert (len(all_noise_indices) == len(all_training_returns) ==
                len(all_training_lengths))

        self.episodes_so_far += num_episodes

        # Assemble the results.
        eval_returns = np.array(all_eval_returns)
        eval_lengths = np.array(all_eval_lengths)
        noise_indices = np.array(all_noise_indices)
        noisy_returns = np.array(all_training_returns)
        noisy_lengths = np.array(all_training_lengths)
        novelty_entropy=np.array(all_novelty)

        eval_acc_returns=np.array(all_eval_acc_returns)
        noisy_acc_returns=np.array(all_training_acc_returns)

        # Process the returns.


        # Compute and take a step.

           #print('enter1')
        population=self.population
        returns_n2=self.returns_n2
        ret=self.ret
        population.extend(all_policy_weight)
        #returns_n2.extend(all_training_returns)

        returns_n2.extend(all_training_acc_returns)
        ret.extend(all_training_returns)
        population.extend(self.policymax)
        returns_n2.extend(self.maxrew)
        ret.extend(self.maxrew2)

        population2=np.array(population)
        returns2_n2=np.array(returns_n2)
        ret2=np.array(ret)

        returns2_n2,indices=np.unique(returns2_n2,return_index=True)
        #population2=population2[indices]

        ret2,ind=np.unique(ret2,return_index=True)
        population2=population2[ind]
        #print('enter2')
        #print('population shape is {}'.format(population2.shape))
        print('returns_n2 is {}'.format(returns_n2))
        print('ret2 is {}'.format(ret2))
        print('returns2_n2 shape is {}'.format(returns2_n2.shape))
        if population2.shape[0]>=config["population_size"]:
            if len(returns2_n2.tolist())>=config["population_size"]:
               idx = np.argpartition(returns2_n2, (-config["population_size"], -1))[-1:-config["population_size"]-1:-1]
	           #population2 = population2[idx]
               returns2_n2=returns2_n2[idx]

            if len(ret2.tolist())>=config["population_size"]:
               idx2 = np.argpartition(ret2, (-config["population_size"], -1))[-1:-config["population_size"]-1:-1]
               ret2=ret2[idx2]
               population2=population2[idx2]

        #print('enter3')
        theta=population2[0][0]
        #print('shape of theta is {}'.format(theta.shape))
        self.population=population2.tolist()
        self.returns_n2=returns2_n2.tolist()
        self.ret=ret2.tolist()

        print("returns_n2 is {}".format(self.returns_n2))
        print("ret2 is {}".format(ret2))

        g=-1000
        update_ratio=-1000


        # Set the new weights in the local copy of the policy.
        self.policy.set_weights(theta)
        self.theta_dict[self.curr_parent] = self.policy.get_weights()


        
        # Store the rewards
        if len(all_eval_returns)>0:
	        if self.curr_parent == 0:
	            self.reward_list1.append(np.mean(eval_returns))

	        if self.curr_parent == 1:
	            self.reward_list2.append(np.mean(eval_returns))

	        if self.curr_parent == 2:
	            self.reward_list3.append(np.mean(eval_returns))

        # Now sync the filters
        FilterManager.synchronize({
            "default": self.policy.get_filter()
        }, self.workers)

        info = {
            "weights_norm": np.square(theta).sum(),
            "grad_norm": np.square(g).sum(),
            "update_ratio": update_ratio,
            "episodes_this_iter": noisy_lengths.size,
            "episodes_so_far": self.episodes_so_far,
        }
        #self.iteration
        
        if self.curr_parent==0:
            self.reward_mean1 = np.mean(self.reward_list1[-self.report_length:])
            # if len(self.reward_list1)>=self.report_length:
	           #   reward_max1=np.max(self.reward_list1[-self.report_length:])
	           #   reward_min1=np.min(self.reward_list1[-self.report_length:])
        if self.curr_parent==1:
            self.reward_mean2 = np.mean(self.reward_list2[-self.report_length:])
            # if len(self.reward_list2)>=self.report_length:
	           #   reward_max2=np.max(self.reward_list2[-self.report_length:])
	           #   reward_min2=np.min(self.reward_list2[-self.report_length:])
        if self.curr_parent==2:
	        self.reward_mean3 = np.mean(self.reward_list3[-self.report_length:])
             # if len(self.reward_list3)>=self.report_length:
	            #  reward_max3=np.max(self.reward_list3[-self.report_length:])
	            #  reward_min3=np.min(self.reward_list3[-self.report_length:])
        # reward_mean_noise=np.mean(all_training_returns)
        # reward_max_noise=np.max(all_training_returns)
        # reward_min_noise=np.min(all_training_returns)
        result = dict(
            #episode_reward_min1=reward_min1,
            episode_reward_mean1=self.reward_mean1,
            #episode_reward_max1=reward_max1,
            #episode_reward_min2=reward_min2,
            episode_reward_mean2=self.reward_mean2,
            #episode_reward_max2=reward_max2,
            #episode_reward_min3=reward_min3,
            episode_reward_mean3=self.reward_mean3,
            #episode_reward_max3=reward_max3,
            # noise_reward_min=reward_min_noise,
            # noise_reward_mean=reward_mean_noise,
            # noise_reward_max=reward_max_noise,
            episode_len_mean=eval_lengths.mean(),
            timesteps_this_iter=noisy_lengths.sum(),
            info=info)
        self.curr_parent = (self.curr_parent + 1) % config["pop_size"] 

        return result

    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for w in self.workers:
            w.__ray_terminate__.remote()

    def __getstate__(self):
        return {
            "weights": self.policy.get_weights(),
            "filter": self.policy.get_filter(),
            "episodes_so_far": self.episodes_so_far,
        }

    def __setstate__(self, state):
        self.episodes_so_far = state["episodes_so_far"]
        self.policy.set_weights(state["weights"])
        self.policy.set_filter(state["filter"])
        FilterManager.synchronize({
            "default": self.policy.get_filter()
        }, self.workers)

    def compute_action(self, observation):
        return self.policy.compute(observation, update=False)[0]
