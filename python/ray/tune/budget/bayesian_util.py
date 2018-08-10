from __future__ import division, print_function
import copy
import random
import numpy as np
import ray.tune.budget.kernel_util as kernel


# from data_util import process_cifar

class BeliefModel:
    """class for belief model
    Freeze-Thaw Gaussian Process https://arxiv.org/pdf/1406.3896.pdf
    code also see https://github.com/jamesrobertlloyd/automl-phase-2
    """

    def __init__(self, x_kernel_params, t_kernel_params, mean=0, thres=5e-3, step=5):
        self.t_kernel = kernel.ft_K_t_t_plus_noise
        self.x_kernel = kernel.cov_iid
        self.x_kernel_params = x_kernel_params
        self.t_kernel_params = t_kernel_params
        self.mean = mean
        self.thres = thres
        self.step = step
        self.tscale = t_kernel_params['tscale']
        print('scale = %g' % self.tscale)

    def set_kernel_params(self, x_kernel_params, t_kernel_params):
        self.x_kernel_params = x_kernel_params
        self.t_kernel_params = t_kernel_params

    def ft_log_likelihood(self, x, t, y, x_kernel_params, t_kernel_params):
        """Freeze thaw log likelihood
        x: a list of different models
        t, y: a list of models, each item in the list is a list of trajectory
        """
        # Take copies of everything - this is a function
        N = len(y)
        m = np.ones((N, 1)) * self.mean
        t = copy.deepcopy(t)
        y = copy.deepcopy(y)
        x = copy.deepcopy(x)

        K_x = self.x_kernel(x, x, **x_kernel_params)

        lambd = np.zeros((N, 1))
        gamma = np.zeros((N, 1))

        K_t = [None] * N

        for n in range(N):
            K_t[n] = self.t_kernel(t[n], t[n], **t_kernel_params)
            lambd[n] = np.dot(np.ones((1, len(t[n]))),
                              np.linalg.solve(K_t[n], np.ones((len(t[n]), 1))))
            # Making sure y[n] is a column vector
            y[n] = np.array(y[n], ndmin=2).T
            # gamma = 1\T inv(K) (y - 1m) -- implicit broadcast for m
            gamma[n] = np.dot(np.ones((1, len(t[n]))), np.linalg.solve(K_t[n], y[n] - m[n]))

        Lambd = np.diag(lambd.ravel())

        ll = 0

        # Terms relating to individual curves
        for n in range(N):
            ll += - 0.5 * np.dot((y[n] - m[n]).T, np.linalg.solve(K_t[n], y[n] - m[n]))
            ll += - 0.5 * np.linalg.slogdet(K_t[n])[1]

        # Terms relating to K_x
        ll += + 0.5 * np.dot(gamma.T, np.linalg.solve(np.linalg.inv(K_x) + Lambd, gamma))
        ll += - 0.5 * np.linalg.slogdet(np.linalg.inv(K_x) + Lambd)[1]
        ll += - 0.5 * np.linalg.slogdet(K_x)[1]

        return ll

    def ft_posterior(self, x, t, y, t_star):
        """Freeze thaw posterior (predictive)
        x: a list of different models
        t, y: a list of models, each item in the list is a list of trajectory
        """
        # Take copies of everything - this is a function
        N = len(y)
        m = np.ones((N, 1)) * self.mean

        x = copy.deepcopy(x)
        t = copy.deepcopy(t)
        y = copy.deepcopy(y)
        t_star = copy.deepcopy(t_star)

        K_x = self.x_kernel(x, x, **self.x_kernel_params)

        lambd = np.zeros((N, 1))
        gamma = np.zeros((N, 1))
        Omega = [None] * N

        K_t = [None] * N
        K_t_t_star = [None] * N

        y_mean = [None] * N

        for n in range(N):
            K_t[n] = self.t_kernel(t[n], t[n], **self.t_kernel_params)
            K_t_t_star[n] = self.t_kernel(t[n], t_star[n], **self.t_kernel_params)
            lambd[n] = np.dot(np.ones((1, len(t[n]))),
                              np.linalg.solve(K_t[n], np.ones((len(t[n]), 1))))
            # Making sure y[n] is a column vector
            y[n] = np.array(y[n], ndmin=2).T
            gamma[n] = np.dot(np.ones((1, len(t[n]))), np.linalg.solve(K_t[n], y[n] - m[n]))
            Omega[n] = np.ones((len(t_star[n]), 1)) - \
                       np.dot(K_t_t_star[n].T, np.linalg.solve(K_t[n], np.ones(y[n].shape)))

        Lambda_inv = np.diag(1 / lambd.ravel())
        C = K_x - np.dot(K_x, np.linalg.solve(K_x + Lambda_inv, K_x))
        mu = m + np.dot(C, gamma)

        K_t_star_t_star = [None] * N
        y_var = [None] * N
        for n in range(N):
            y_mean[n] = np.dot(K_t_t_star[n].T, np.linalg.solve(K_t[n], y[n])) + Omega[n] * mu[n]

            K_t_star_t_star[n] = self.t_kernel(t_star[n], t_star[n], **self.t_kernel_params)
            y_var[n] = K_t_star_t_star[n] - \
                       np.dot(K_t_t_star[n].T, np.linalg.solve(K_t[n], K_t_t_star[n])) + \
                       C[n, n] * np.dot(Omega[n], Omega[n].T)
        return y_mean, y_var

    def predict_xiid(self, xx, tt, yy, Budgets, oldhyp, sampler, debug=False):
        N = len(xx)
        # check budget in the right format
        if (not isinstance(Budgets, list)) or len(Budgets) == 1:
            Budgets = np.ones(N) * Budgets

        # # Sample hyper parameters
        # keep only models with observations
        predicate = [len(i) != 0 for i in tt]
        x_ = [xx[i] for i in range(N) if predicate[i]]
        t_ = [tt[i] for i in range(N) if predicate[i]]
        y_ = [yy[i] for i in range(N) if predicate[i]]

        if x_:
            # hyp [alpha, beta, scale, log_noise, x_scale]
            logdist = lambda hyp: self.ft_log_likelihood(x_, t_, y_,
                                                         x_kernel_params=dict(scale=hyp[4]),
                                                         t_kernel_params=dict(alpha=hyp[0],
                                                                              beta=hyp[1],
                                                                              scale=hyp[2],
                                                                              log_noise=hyp[3],
                                                                              tscale=self.tscale))

            # get sampling hyperparameter
            # sampler hyper parameter: bounds, nsample=1, burn=10, widths=0.5, max_attempts=10
            samples, ever_fail = slice_sample_bounded_max(logdist, oldhyp, sampler, debug=debug)
            # TODO: more than one sample???
            newhyp = samples[-1]
            ll = logdist(newhyp)
            ll = ll.ravel()[0]
            # TODO: change hard code!!!
            fail = np.sum(ever_fail > 2) > sampler['burn'] / 2.0 or np.isinf(ll)
            # if debug is True:
            print('fail is %s, likelihood %g' % (fail, ll))
        else:
            newhyp = oldhyp
            print('no sampling this iter.')
            fail = False

        # Setup params
        self.set_kernel_params(x_kernel_params=dict(scale=newhyp[4]),
                               t_kernel_params=dict(alpha=newhyp[0],
                                                    beta=newhyp[1],
                                                    scale=newhyp[2],
                                                    log_noise=newhyp[3],
                                                    tscale=self.tscale))

        t_star_ = [np.arange(tt[i][-1] + 1, Budgets[i]) for i in range(N) if predicate[i]]

        y_mean, y_covar = self.ft_posterior(x_, t_, y_, t_star_)
        y_mu = [None] * N
        y_var = [None] * N
        t_star = [None] * N
        j = 0
        for i in range(N):
            if predicate[i]:
                y_mu[i] = y_mean[j].ravel()
                y_var[i] = np.diag(y_covar[j])
                t_star[i] = t_star_[j]
                if len(y_mu) <= 1:
                    raise Exception("belief model error: y_mu length should be greater than 1.")
                j += 1
            else:
                y_mu[i] = np.inf
                y_var[i] = np.inf
                t_star[i] = Budgets[i]
        # assert j == np.sum(predicate)
        return t_star, y_mu, y_var, newhyp, fail

    def convergence(self, t, mu, var):
        T = np.zeros(len(mu))
        f = np.zeros(len(mu))
        v = np.zeros(len(mu))

        for idx, x in enumerate(mu):
            if not isinstance(x, np.ndarray):
                # we don't have observation yet
                T[idx] = t[idx]
                f[idx] = mu[idx]
                v[idx] = var[idx]
            else:
                # x is the future prediction sequence
                d = -np.diff(x)
                didx = np.argmax(d < self.thres)
                ll = len(x)
                if didx == 0:
                    didx = ll - 1
                i = int(min(np.ceil((didx + 1) / self.step) * self.step, ll - 1))
                T[idx] = t[idx][i]
                f[idx] = mu[idx][i]
                v[idx] = var[idx][i]
        return T, f, v

    def predict_asymptote(self, xx, tt, yy, maxL, oldhyp, sampler, debug=False):
        N = len(xx)
        # # Sample hyper parameters
        # keep only models with observations
        predicate = [len(i) != 0 for i in tt]
        x_ = [xx[i] for i in range(N) if predicate[i]]
        t_ = [tt[i] for i in range(N) if predicate[i]]
        y_ = [yy[i] for i in range(N) if predicate[i]]

        if x_:
            # hyp [alpha, beta, scale, log_noise, x_scale]
            logdist = lambda hyp: self.ft_log_likelihood(x_, t_, y_,
                                                         x_kernel_params=dict(scale=hyp[4]),
                                                         t_kernel_params=dict(alpha=hyp[0],
                                                                              beta=hyp[1],
                                                                              scale=hyp[2],
                                                                              log_noise=hyp[3],
                                                                              tscale=self.tscale))

            # get sampling hyperparameter
            # sampler hyper parameter: bounds, nsample=1, burn=10, widths=0.5, max_attempts=10
            samples, ever_fail = slice_sample_bounded_max(logdist, oldhyp, sampler, debug=debug)
            # TODO: more than one sample???
            newhyp = samples[-1]
            ll = logdist(newhyp)
            ll = ll.ravel()[0]
            # TODO: change hard code!!!
            fail = np.sum(ever_fail > 2) > sampler['burn'] / 2.0 or np.isinf(ll)
            # if debug is True:
            print('fail is %s, likelihood %g' % (fail, ll))
        else:
            newhyp = oldhyp
            print('no sampling this iter.')
            fail = False

        # Setup params
        self.set_kernel_params(x_kernel_params=dict(scale=newhyp[4]),
                               t_kernel_params=dict(alpha=newhyp[0],
                                                    beta=newhyp[1],
                                                    scale=newhyp[2],
                                                    log_noise=newhyp[3],
                                                    tscale=self.tscale))

        t_star_ = [np.array([maxL]) for i in range(N) if predicate[i]]

        y_mean, y_covar = self.ft_posterior(x_, t_, y_, t_star_)
        y_mu = np.zeros(N)
        y_var = np.zeros(N)

        j = 0
        for i in range(N):
            if predicate[i]:
                y_mu[i] = y_mean[j].ravel()
                y_var[i] = y_covar[j].ravel()
                if len(y_mu) <= 1:
                    raise Exception("belief model error: y_mu length should be greater than 1.")
                j += 1
            else:
                y_mu[i] = np.inf
                y_var[i] = np.inf

        # assert j == np.sum(predicate)
        return y_mu, y_var, newhyp, fail

    def ft_asymptote(self, xx, tt, yy):
        N = len(yy)
        m = np.ones((N, 1)) * self.mean

        x = copy.deepcopy(xx)
        t = copy.deepcopy(tt)
        y = copy.deepcopy(yy)

        K_x = self.x_kernel(x, x, **self.x_kernel_params)

        lambd = np.zeros((N, 1))
        gamma = np.zeros((N, 1))
        K_t = [None] * N

        for n in range(N):
            K_t[n] = self.t_kernel(t[n], t[n], **self.t_kernel_params)
            lambd[n] = np.dot(np.ones((1, len(t[n]))),
                              np.linalg.solve(K_t[n], np.ones((len(t[n]), 1))))
            # Making sure y[n] is a column vector
            y[n] = np.array(y[n], ndmin=2).T
            gamma[n] = np.dot(np.ones((1, len(t[n]))),
                              np.linalg.solve(K_t[n], y[n] - m[n]))

        Lambda_inv = np.diag(1 / lambd.ravel())
        C = K_x - np.dot(K_x, np.linalg.solve(K_x + Lambda_inv, K_x))
        mu = m + np.dot(C, gamma)
        return mu, C


def slice_sample_bounded_max(logdist, oldhyp, sampler, step_out=True, debug=False):
    """
    Slice sampling with bounds and max iterations
    See Pseudo-code in David MacKay's text book p375
    Adapted from James Lloyd's 2015 code
    """
    # expand sampler hyperparameter
    N = sampler.setdefault('nsample', 1)
    burn = sampler.setdefault('burn', 10)
    widths = sampler.setdefault('widths', 0.5)
    max_attempts = sampler.setdefault('max_attempts', 10)
    bounds = sampler['bounds']

    oldhyp = copy.deepcopy(oldhyp)
    D = len(oldhyp)
    samples = []
    if (not isinstance(widths, list)) or len(widths) == 1:
        widths = np.ones(D) * widths

    log_Px = logdist(oldhyp)

    ever_fail = np.zeros(N + burn)
    for ii in range(N + burn):
        # sample u prime
        log_uprime = np.log(random.random()) + log_Px

        if debug is True:
            print \
                ('oldhyp/bound = %s' % " ".join(
                    ["%.2f/(%.2f, %.2f)" % (oldhyp[s], bounds[s][0], bounds[s][1])
                     for s in range(len(oldhyp))]))
            print('Current ll = %.2f, Slice = %.2f. ' % (log_Px, log_uprime))
        for dd in random.sample(range(D), D):
            x_l = copy.deepcopy(oldhyp)
            x_r = copy.deepcopy(oldhyp)
            xprime = copy.deepcopy(oldhyp)

            # Create a horizontal interval (x_l, x_r) enclosing oldhyp
            rr = random.random()
            x_l[dd] = max(oldhyp[dd] - rr * widths[dd], bounds[dd][0])
            x_r[dd] = min(oldhyp[dd] + (1 - rr) * widths[dd], bounds[dd][1])

            if step_out:
                steps = 0
                while logdist(x_l) > log_uprime and x_l[dd] > bounds[dd][0]:
                    if debug is True and x_l[3] > 0:
                        print('Large noise')
                    x_l[dd] = max(x_l[dd] - widths[dd], bounds[dd][0])
                    steps += 1
                    if steps > max_attempts:
                        break
                steps = 0
                while logdist(x_r) > log_uprime and x_r[dd] < bounds[dd][1]:
                    x_r[dd] = min(x_r[dd] + widths[dd], bounds[dd][1])
                    steps += 1
                    if steps > max_attempts:
                        break

            # Loop proposing xprimes and shrink interval
            num_attempts = 0
            fail = False
            while True:
                xprime[dd] = random.random() * (x_r[dd] - x_l[dd]) + x_l[dd]

                if debug is True and xprime[3] > 0:
                    print('Large noise')
                log_Px = logdist(xprime)
                if log_Px > log_uprime:
                    oldhyp[dd] = xprime[dd]
                    break
                else:
                    # Shrink in
                    num_attempts += 1
                    if num_attempts >= max_attempts:
                        print('dim %d Failed to find something' % dd)
                        ever_fail[ii] += 1
                        break
                    elif xprime[dd] > oldhyp[dd]:
                        x_r[dd] = xprime[dd]
                    elif xprime[dd] < oldhyp[dd]:
                        x_l[dd] = xprime[dd]
                    else:
                        raise Exception('Slice sampling failed to find an acceptable point')
        # Record samples
        if ii >= burn:
            samples.append(copy.deepcopy(oldhyp))
            print('newhyp = %s' % " ".join(["%.2f" % (oldhyp[s]) for s in range(len(oldhyp))]))
    return samples, ever_fail