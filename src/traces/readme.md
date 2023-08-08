# Sample Trace Files

## Intervals

```intervals/``` contains the interval (timing) traces used by our experiments. Each line represents a new operation arriving to the graph.
The number represents an offset in milliseconds from the beginning of the program. 
For example, if line 10 contains the value ``100``, that means the 10th operation arrives 100ms after the start of the program.

The following files are included in the directory:
- ```intervals-10k-0-10.txt```: 10k operations with uniform arrival rate from [0ms, 10ms] ("underprovisioned")
- ```intervals-10k-290-300.txt```: 10k operations uniform arrival rate from [290ms, 300ms] ("overprovisioned")
- ```intervals-fluct-10k-0-10-90-100.txt```: 10k operations fluctuating between uniform arrival rates [0ms, 10ms] & [90ms, 100ms]
- ```intervals-fluct-10k-0-10-190-200.txt```: 10k operations fluctuating between uniform arrival rates [0ms, 10ms] & [190ms, 200ms]
- ```intervals-fluct-10k-0-10-290-300.txt```: 10k operations fluctuating between uniform arrival rates [0ms, 10ms] & [290ms, 300ms]


## Operation Stream
```operations/``` contains the operation streams used for the read-only and read-write experiments on the Twitter dataset (which can be found in ```src/twitter-dataset```.
Each line is an operation, following one of these formats:

- ```find <userId>```
- ```getFollowers <userId>```
- ```getFollowees <userId```
- ```add userId <userIdToFind> userId <userIdToAdd> <relationshipDirectionToExisting>```
- ```delete <userId>```
- ```update userId <userId> <newPropertyName> <newPropertyValue>```
