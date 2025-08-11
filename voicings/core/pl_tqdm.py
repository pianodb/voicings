# https://stackoverflow.com/a/75922391/29032885

import polars as pl
from tqdm import tqdm

def w_pbar(pbar, func):
    def foo(*args, **kwargs):
        pbar.update(1)
        return func(*args, **kwargs)

    return foo

# with tqdm(total=num_groups) as pbar:
#    res = df.group_by('team').map_groups(w_pbar(pbar, lambda x: x.select(pl.col('points').mean())))
