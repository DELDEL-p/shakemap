import numpy as np
import pandas as pd

def make_features(X: pd.DataFrame):
    dist = np.asarray(X["dist"], dtype=float)
    pga  = np.asarray(X["pga"], dtype=float)

    log_pga  = np.log10(pga + 1e-6)
    log_dist = np.log10(dist + 1.0)

    inv_dist = 1.0 / (dist + 1.0)
    pga_over_dist = log_pga - log_dist
    log_dist2 = log_dist ** 2

    return np.c_[log_pga, log_dist, inv_dist, pga_over_dist, log_dist2]
