# Code in this file is copied and adapted from
# https://github.com/openai/evolution-strategies-starter.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import numpy as np
import tensorflow as tf

import ray
from ray.rllib.evaluation.sampler import _unbatch_tuple_actions
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.filter import get_filter

import scipy.signal
from sklearn.neighbors import NearestNeighbors
import math

def discount(x, gamma=0.99):
    return scipy.signal.lfilter([1], [1, -gamma], x[::-1], axis=0)[::-1]

# def novel(x):
#     x=np.array(x)
#     seq=[]
#     for t in range(x.size):
#         sequence=1-0.5*math.exp(-t)
#         seq.append(sequence)
    
#     #print("seq={}".format(seq))


#     novelty=[]

#     for i in range(x.size):
#         if i==0:
#             novelty.append(np.dot(seq,x[i:]))
#         else:
#             novelty.append(np.dot(seq[:-(i)],x[i:]))

#     return novelty

def cross_entropy(p,q,n):
    total=0
    # p=p[0]
    # q=q[0]

    for i in range(n):
        if q[i]<0.0001:
            q[i]=q[i]+0.0001
        total+=-p[i]*np.log(q[i])
    return total

def entropy(p,n):
    total=0
    #print('p is {}'.format(p))
    for i in range(n):
        if p[i] >0.0001:
            total+=-p[i]*np.log(p[i])
    return total

def euclidean_distance(x, y):
    n, m = len(x), len(y)
    if n > m:
        a = np.linalg.norm(y - x[:m])
        b = np.linalg.norm(y[-1] - x[m:])
    else:
        a = np.linalg.norm(x - y[:n])
        b = np.linalg.norm(x[-1] - y[n:])
    return np.sqrt(a**2 + b**2)

def KNN(k,memory,s,a,env):
    mem=np.array(memory)
    memstate=mem[:,0:-1]
    memaction=mem[:,-1]

    k=min(k,len(memory))

    neigh=NearestNeighbors(n_neighbors=k)
    neigh.fit(memstate)
    #ind=neigh.kneighbors([s],return_distance=False)
    dist,ind=neigh.kneighbors([s])
    dis=np.array(dist[0])
    dis=np.mean(dis)
    entro_diff=dis
    #top_k_indices=ind[0]

    #top_k_states=memstate[top_k_indices]

    #top_k_actions=memaction[top_k_indices]

    # countaction=np.zeros([env.action_space.n])

    # for i in range(env.action_space.n):
    #     #print('countaction is {}'.format(countaction))
    #     countaction[i]=list(top_k_actions).count(i)
    
    # probaction=list(countaction/np.sum(countaction))
    
    # countaction2=countaction
    # countaction2[a]+=1

    # probaction2=list(countaction2/np.sum(countaction2))

    # entro_diff=0.05/(probaction[a]+0.01)

    return entro_diff

def distance(group):
    if len(group)>1:
        neinum=len(group)-1
    else:
        neinum=1
    neigh=NearestNeighbors(n_neighbors=len(group)-1)
    neigh.fit(group[1:])
    dist,ind=neigh.kneighbors([group[0]])
    dis=np.array(dist[0])
    dis=np.mean(dis)

    return dis
 

def rollout(policy,config,env,timestep_limit=None, add_noise=False):
    """Do a rollout.
    
    If add_noise is True, the rollout will take noisy actions with
    noise drawn from that stream. Otherwise, no action noise will be added.
    """

    env_timestep_limit = env.spec.max_episode_steps
    timestep_limit = (env_timestep_limit if timestep_limit is None else min(
        timestep_limit, env_timestep_limit))
    rews = []
    entros=[]
    rew_chg=[]
    entro_chg=[]
    ramstate=[]
    dist=[]
    t = 0
    observation = env.reset()
    for i in range(timestep_limit or 999999):
        #ac= policy.compute(observation, add_noise=add_noise)[0]
        ac,entro,ac_dist,prob= policy.compute(observation, add_noise=add_noise)
        
        # listprob=prob[0].tolist()
        # ac_idx=listprob.index(max(listprob))
        # ac=[ac_idx]
        probability=[]
        probability1=config["epsilon"]/env.action_space.n
        probability2=config["epsilon"]/env.action_space.n+1-config["epsilon"]
        listprob=prob[0].tolist()
        ac_idx=listprob.index(max(listprob))
        for idx in range(env.action_space.n):
            if idx is ac_idx:
               probability.append(probability2)
            else:
               probability.append(probability1)
        ac=np.random.multinomial(1,probability)
        ac=ac.tolist()
        ac=[ac.index(max(ac))]

        ac=ac[0]
        # q=[]
        # for idx in range(env.action_space.n):
        #     if idx is ac:
        #         q.append(1)
        #     else:
        #         q.append(0)
        # entro_diff=cross_entropy(q,prob[0],env.action_space.n)
        # entro_chg.append(entro_diff)
        observation, rew, done, _ = env.step(ac)

        # if ac is not ac_idx:
        #      rew=rew


        entros.extend(entro)
        rews.append(rew)
        t += 1
        if done:
            break
    rews = np.array(rews, dtype=np.float32)
    entros=np.array(entros,dtype=np.float32)
    entro_chg=np.array(entro_chg,dtype=np.float32)

    returns=discount(rews,1)
    novelty=discount(entros,1)  

    #returns=returns-abs(entro_chg)  
    #rews=rews-abs(entro_chg)
    return novelty,returns,rews, t

def get_ref_batch(env, batch_size):
    ref_batch = []
    observation = env.reset()
    while len(ref_batch) < batch_size:
        observation, rew, done, info= env.step(env.action_space.sample())
        ref_batch.append(observation)
        if done:
            observation = env.reset()
    return ref_batch


class GenericPolicy(object):
    def __init__(self, sess, env,env2,action_space, obs_space, preprocessor,
                 observation_filter, model_options, action_noise_std):
        self.sess = sess
        self.action_space = action_space
        self.action_noise_std = action_noise_std
        self.preprocessor = preprocessor
        self.observation_filter = get_filter(observation_filter,
                                             self.preprocessor.shape)
        self.inputs = tf.placeholder(tf.float32,
                                     [None] + list(self.preprocessor.shape))
        self.batches=tf.placeholder(tf.float32,[None]+list(self.preprocessor.shape))

        # Policy network.
        dist_class, dist_dim = ModelCatalog.get_action_dist(
            self.action_space, model_options, dist_type="deterministic")
        # model = ModelCatalog.get_model({
        #     "obs": self.inputs, "batch":self.batches
        #    }, obs_space, dist_dim, model_options)

        self.model = ModelCatalog.get_model({
            "obs": self.inputs}, obs_space, dist_dim, model_options)
        self.dist = dist_class(self.model.outputs)
        self.sampler = self.dist.sample()
        self.entro=self.dist.entropy()
        self.prob=self.dist.softmax()

        
        self.ref_list = []
        self.refobservation=[]

        if isinstance(env.action_space,gym.spaces.Discrete):
            self.ref_batch=get_ref_batch(env2,batch_size=16)
            self.refobservation=self.set_ref_batch(self.ref_batch)

        self.variables = ray.experimental.TensorFlowVariables(
            self.model.outputs, self.sess)

        self.num_params = sum(
            np.prod(variable.shape.as_list())
            for _, variable in self.variables.variables.items())
        #self.sess.run(tf.global_variables_initializer())

    def set_ref_batch(self, ref_batch):
        
        self.ref_list = self.ref_batch
        #print('ref_list_length={}'.format(len(self.ref_list)))
        for ref in self.ref_list:
            refobs= self.preprocessor.transform(ref)
            refobs= self.observation_filter(refobs[None],update=True)
            self.refobservation.extend(refobs.tolist())
        self.refobservation = np.array(self.refobservation)
        return self.refobservation
        
    def compute(self, observation,add_noise=False, update=True):
        observation = self.preprocessor.transform(observation)
        observation = self.observation_filter(observation[None], update=update)
        # if isinstance(self.action_space,gym.spaces.Discrete):
        #    #print('enter compute action') 
        #    action = self.sess.run(
        #     self.sampler, feed_dict={self.inputs: observation, self.batches: self.refobservation})
        #    entro = self.sess.run(self.entro,feed_dict={self.inputs: observation, self.batches: self.refobservation})
        #    #print('exit compute action') 
        # else:
        prob=self.sess.run(self.prob,feed_dict={self.inputs: observation})
        action_distribution=self.sess.run(self.model.outputs,feed_dict={self.inputs: observation})
        action = self.sess.run(self.sampler, feed_dict={self.inputs: observation})  
        # prob1=prob[0]
        # prob1=np.array(prob1,dtype=np.float64)

        # prob1=prob1/np.array(prob1).sum()
        # prob1=list(prob1)
        #print('prob1={}, sumprob1={}'.format(prob1,np.array(prob1).sum()))
        # actionarray=np.random.multinomial(1,prob1)
        # actionlist=actionarray.tolist()
        # action=[actionlist.index(max(actionlist))]
        #print('prob1 is {}, action is {}'.format(prob1,action))
        entro = self.sess.run(self.entro,feed_dict={self.inputs: observation})
        action = _unbatch_tuple_actions(action)
        if add_noise and isinstance(self.action_space, gym.spaces.Box):
            action += np.random.randn(*action.shape) * self.action_noise_std
        return action,entro,action_distribution,prob

    def set_weights(self, x):
        self.variables.set_flat(x)

    def get_weights(self):
        return self.variables.get_flat()

    def get_filter(self):
        return self.observation_filter

    def set_filter(self, observation_filter):
        self.observation_filter = observation_filter
