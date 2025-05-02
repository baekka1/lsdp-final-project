Clustering Results Analysis

## Using original PIVOT ranking
### Disagreement Results
| Dataset | Disagreements Trial 1 | Disagreements Trial 2 | Disagreements Trial 3 | Average Disagreements |
|---------|---------------------|---------------------|---------------------|-------------------|
| log_normal_100 | 2251 | 2263 | 2253 |2256  |
| musae_ENGB_edges | 43,845 | 39,002 |  |  |
| soc-pokec-relationships | | | | |
| com-orkut.ungraph | | | | |

### Timing Results (Seconds)
| Dataset | Trial 1 | Trial 2 | Trial 3 | Average Time |
|---------|---------|---------|---------|--------------|
| log_normal_100 | 0.014 | 0.011 | 0.012 | 0.0123 |
| musae_ENGB_edges | 0.012 | 0.017 |  |  |
| soc-pokec-relationships | | | | |
| com-orkut.ungraph | | | | |

## Using random rank
### Disagreement Results
| Dataset | Disagreements Trial 1 | Disagreements Trial 2 | Disagreements Trial 3 | Average Disagreements |
|---------|---------------------|---------------------|---------------------|-------------------|
| log_normal_100 | 2357 | 2297 | 2294 | 2316 |
| musae_ENGB_edges | 39,858 | 38,745 | 39,077 | 39,227 |
| soc-pokec-relationships | 29,691,948 | 29,428,696 | 29,908,772 | 29,676,472 |
| soc-LiveJournal1 | 51,969,799 | 51,527,018 | 52,129,550 | 51,875,456 |
| twitter_original_edges | | | | |
| com-orkut.ungraph | | | | |

### Timing Results (seconds)
| Dataset | Trial 1 | Trial 2 | Trial 3 | Average Time |
|---------|---------|---------|---------|--------------|
| log_normal_100 | 3.65 | 3.62 | 5.13 | 4.13 |
| musae_ENGB_edges | 4.60 | 4.26 | 5.77 | 4.88 |
| soc-pokec-relationships | 115.14 | 219.31 | 358.40 | 230.95 |
| soc-LiveJournal1 | 985.86 | 1687.05 | 3167.57 | 1946.83 |
| twitter_original_edges | 806.64 | 2022.62 | | |
| com-orkut.ungraph | | | | |

## Best Disagreement Results
| Dataset| Number of Diagreements| Time | Method of Computation|
|--------|-----------------------|------|----------------------|
|log_normal_100| | | |
| musae_ENGB_edges ||||
|soc-pokec-relationships ||||
|soc-LiveJournal1 ||||
|twitter_original_edges ||||
|com-orkut.ungraph ||||

## Description of Approach
In our approach, we used the parallel PIVOT algorithm along with inner local
search as described in [paper]. The parallel PIVOT algorithm assigns vertices a
random permutation from 1 to N, then iterates over all the vertices and chooses
the vertex with the lowest assigned permutation out of all of its neighbors as
the pivot. Vertices then join their nearest pivot neighbor (if applicable) and
they form a cluster. Repeat until all the vertices have been clustered. After a
clustering has been determined, we run inner local search on all the clusters in
order to reduce the number of disagreements. [more on inner local search if we
get that working] 

With the parallelized PIVOT and local search, we were able to get pretty good
results. As both are parallelizable, we started with this approach because we
knew that the algorithm that we used had to scale to much larger graphs. 

## Discussion of Algorithm
The parallized PIVOT algorithm gives a $3$-approximation solution in expectation and takes $O(\log ^2 n)$
rounds through an analysis from *Blelloch, Fineman, Shun '12*


