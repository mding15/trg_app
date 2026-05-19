
import sys
import numpy as np
import pandas as pd
from pathlib import Path
# sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
# sys.path.insert(0, r'C:\dev\claude\trg_app')


from utils import var_utils

# cf_security_id = 'T10001431'
# security_id_list = ['T10001310']
# corrs = [0.5]
# vols = [0.0081102902167981]
# adj_factors=[1.75895713905632]
             
def simulate_dist(security_id_list, cf_security_id, corrs, vols, adj_factors): 
    
    # simulate security distributions
    cf_dist = var_utils.get_dist([cf_security_id])
    cf_vol = cf_dist.std().iloc[0]

    # calculate beta and sigma for each security
    betas, sigmas = [],[]
    for sec_id, corr, vol, adj_factor in zip(security_id_list, corrs, vols, adj_factors):
        vol = vol * adj_factor
        beta = corr * (vol / cf_vol)
        sigma = np.sqrt(vol**2 - beta**2 * cf_vol**2)
        betas.append(beta)
        sigmas.append(sigma)
        print(f'Security: {sec_id}, Beta: {beta}, Sigma: {sigma}, Adj Factor: {adj_factor}')
    
    # simulate security distribution
    sys_dist = cf_dist.reset_index(drop=True).iloc[:,0]
    N = len(sys_dist)
    dist = {cf_security_id: sys_dist}
    for sec_id, beta, sigma in zip(security_id_list, betas, sigmas):
        print(beta, sigma)
        dist[sec_id] = sys_dist * beta + np.random.normal(0, sigma, N)
    
    dist = pd.concat(dist, axis=1)    
 
    return dist