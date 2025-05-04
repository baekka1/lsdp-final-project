# Clustering Results Analysis
By Katie Baek and Rafael Singer

## Best Disagreement Results
| Dataset| Number of Diagreements| Time | Algorithm |Method of Computation|
|--------|-----------------------|------|-----------|-----------|
|log_normal_100| 1769 | 2.54 seconds| PIVOT + 50 iterations of LS |Local|
| musae_ENGB_edges | 34266|45.4 seconds | PIVOT + 40 iterations of LS |GCP - N4 Series with 2x2 Cores|
|soc-pokec-relationships | 26208949 | 90 minutes|PIVOT + 10 iterations of LS | GCP - E2 Series with 4x2 Cores|
|soc-LiveJournal1 |48254893 | 54 minutes |PIVOT + 3 iterations of LS| GCP - E2 Series with 4 x 2 Cores |
|twitter_original_edges | 70924289 |50 minutes | PIVOT + 1 iterations of LS | GCP - E2 Series with 4 x 2 Cores|
|com-orkut.ungraph | 152098278| 57 minutes|PIVOT + 2 iterations of LS| GCP - E2 Series
with 4 x 2 Cores|


## Description of Approach
In our approach, we used the parallel PIVOT algorithm along with inner local
search as described in [paper]. The original parallel PIVOT algorithm assigns vertices a
random permutation from 1 to N, then iterates over all the vertices and chooses
the vertex with the lowest assigned permutation out of all of its neighbors as
the pivot. Vertices then join their nearest pivot neighbor (if applicable) and
they form a cluster. Repeat until all the vertices have been clustered. 
In our implementation, we modified the algorithm so that instead of assigning
the permutation described in the paper, we did it according to a hash function.
Initially, this was to introduce a greater range of numbers and to ensure
reproducability but we found through experimentation that this produced a
smaller number of disagreements than the randomly generated assignments so we
kept the hash function. This approach is not perfect as with much larger graphs,
the odds of two vertices being hashed to the number increases but we decided
that this was an acceptable cost of getting the algorithm to run with less
overhead and faster. In addition, instead of keeping track of all nodes that
have been unassigned, our algorithm stops when no new clusters have been formed.
While this may result in the algorithm stopping prematurely, it avoids a
`.count()` or some other similar computation which we found to down the process
significantly. 


After a clustering has been determined, we run local search on the graph that
our PIVOT algorithm outputs. Local search (LS) considers the cost/benefit of 'moving'
a vertex from one cluster to another, based off of a cost function described as 
$nD-nC+1+2(a-b)$, where $nD$ and $nC$ are size of the current cluster and target
cluster, $a$ is a vertices' neighbors in the current cluster and $b$ is a
vertices' neighbors in the target cluster. The version of LS found in the literature is hard
to parallelize, since a vertex's decision to move is based on the clustering of 
other vertices and thus must be performed sequentially. So instead, we came up with a PIVOT-like version of LS which
ensures that every vertex that is moved is in its own indepedent set. This way,
LS can be parallelized and multiple vertices can move at a time safely since it
is a constraint that they be in their own 'set'. LS is then repeated a certain
number of iterations, or until no other improvements can be made. 

We chose this approach because it scales to much larger graphs with the addition
of more machines, as we found with the orkurt graph. We also found that our
original non-PIVOT LS worked better with the smaller graphs so if we were to run
our algorithms on graphs such as `log_normal_100.csv`, then we would opt for
that one over the PIVOT LS. 

## Theoretical Merits
The parallized PIVOT algorithm which we based our algorithm off of gives a $3$-approximation solution in expectation and takes $O(\log ^2 n)$
rounds through an analysis from *Blelloch, Fineman, Shun '12*. In addition, the
Local Search has a runtime of $O((|C_i|+|E_i^+|))$ per iteration, giving a total
runtime of $O((|V|+|E^+|)I)$ where I is the total number of LS iterations, as shown by *Corder and
Kollios*. However, with our changes to the algorithm, including getting rid of
the true randomness, the approximation probably does not hold. 

## Misc.
The code provided contains all our of approaches, including with the original
permuation and InnerLocalSearch attempts. The `log_100_main.scala` file is the
one used for the `log_normal_100.csv` file. 

## GDrive Link to Solutions
https://drive.google.com/drive/folders/1EEep0zwKYyUnGWfY_RNhA-vO4Tayno_f?usp=sharing
