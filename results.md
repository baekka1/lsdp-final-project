Clustering Results Analysis

## Best Disagreement Results
| Dataset| Number of Diagreements| Time | Method of Computation|
|--------|-----------------------|------|----------------------|
|log_normal_100| 1769 | 2.54 seconds| Local|
| musae_ENGB_edges | 34266|45.4 seconds |GCP - N4 Series with 2x2 Cores|
|soc-pokec-relationships | | ||
|soc-LiveJournal1 | | ||
|twitter_original_edges | | ||
|com-orkut.ungraph | | ||

## Description of Approach
In our approach, we used the parallel PIVOT algorithm along with inner local
search as described in [paper]. The original parallel PIVOT algorithm assigns vertices a
random permutation from 1 to N, then iterates over all the vertices and chooses
the vertex with the lowest assigned permutation out of all of its neighbors as
the pivot. Vertices then join their nearest pivot neighbor (if applicable) and
they form a cluster. Repeat until all the vertices have been clustered. 
In our implementation, we modified the algorithm so that instead of assigning
the permutation described in the paper, we simplied assigned each vertex a
random number from 0 to 1. We went with this approach in order for our algorithm
to be more scalable. The proceedure to get the permutation from 1 to N involved
assigning to each vertex a random number from 0 to 1 and sorting the vertices
according to that assignment. The rank is then given in that order. However,
this is essentially the same thing as doing the random 0 to 1 assignment and
since it is stored as a Double, the odds of two vertices getting assigned the
same number are small. This approach is not perfect as with much larger graphs,
the odds of two vertices getting the same assignment increases but we decided
that this was an acceptable cost of getting the algorithm to run with less
overhead and faster. In addition, instead of keeping track of all nodes that
have been unassigned, our algorithm stops when no new clusters have been formed.
While this may result in the algorithm stopping prematurely, it avoids a
`.count()` or some other similar computation and will speed up the algorithm.


After a clustering has been determined, we run inner local search on all the clusters in
order to reduce the number of disagreements. We chose inner local search because
through our experimentations, local search proved to be too memory and
computationally expensive. With inner local search, we only consider the
vertices and edges of one cluster at a time instead of the whole graph. In the
ppaer by *Corder and Kollios '23*, they showed that this approach is scalable
and produces as good as, or better clusterings than regular local search. 



With the parallelized PIVOT and local search, we were able to get pretty good
results. As both are parallelizable, we started with this approach because we
knew that the algorithm that we used had to scale to much larger graphs. 

## Discussion of Algorithm
The parallized PIVOT algorithm gives a $3$-approximation solution in expectation and takes $O(\log ^2 n)$
rounds through an analysis from *Blelloch, Fineman, Shun '12*. In addition, the
Inner Local Search has a runtime of $O((|C_i|+|E_i^+|)I_i)$ per cluster $C_i$,
where $I_i$ is the number of Local Search iterations that is run per cluster.
That brings the total time complexity for a graph $G$ as
$O(|V|+|E^+|+\sum_{i=1}^k (|C_i|+|E_i^+})I_i)$, as shown by *Corder and
Kollios*.


