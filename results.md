# Clustering Results Analysis

## Best Disagreement Results
| Dataset| Number of Diagreements| Time | Method of Computation|
|--------|-----------------------|------|----------------------|
|log_normal_100| 1769 | 2.54 seconds| Local|
| musae_ENGB_edges | 34266|45.4 seconds |GCP - N4 Series with 2x2 Cores|
|soc-pokec-relationships | | ||
|soc-LiveJournal1 | | ||
|twitter_original_edges | | ||
|com-orkut.ungraph | | ||

Discussion of the merits of your algorithms such as the theoretical merits (i.e. if you can show your algorithm has certain guarantee).
The scalability of your approach
Depth of technicality
Novelty
Completeness
Readability

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
`.count()` or some other similar computation which we found to down the process
significantly. 


After a clustering has been determined, we run local search on the graph that
our PIVOT algorithm outputs. Local search (LS) considers the cost/benefit of 'moving'
a vertex from one cluster to another. The version of LS found in the literature is hard
to parallelize, since a vertex's decision to move is based on the clustering of 
other vertices. 



With the parallelized PIVOT and local search, we were able to get pretty good
results. As both are parallelizable, we started with this approach because we
knew that the algorithm that we used had to scale to much larger graphs. 

## Theoretical Merits
The parallized PIVOT algorithm gives a $3$-approximation solution in expectation and takes $O(\log ^2 n)$
rounds through an analysis from *Blelloch, Fineman, Shun '12*. In addition, the
Inner Local Search has a runtime of $O((|C_i|+|E_i^+|)I_i)$ per cluster $C_i$,
where $I_i$ is the number of Local Search iterations that is run per cluster.
That brings the total time complexity for a graph $G$ as
$O(|V|+|E^+|+\sum_{i=1}^k (|C_i|+|E_i^+|)I_i)$, as shown by *Corder and
Kollios*.


