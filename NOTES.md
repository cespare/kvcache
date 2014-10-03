## GC notes

Goal: lower the pause time.

Here are some microbenchmarks. Methodology:

1. Create a big map
2. `runtime.GC()`
3. `runtime.GC()` and look at the reported pause time in `runtime.MemStats`

The time of the second collection is mostly just affected by how many pointers are on the heap. Here are some
benchmark results for 1M-entry maps (pause times in ms):

```
map[string]*struct{[]byte}	    160
map[int]*struct{[]byte}	        132
map[string]struct{[]byte}	      75
map[string]*struct{[100]byte}	  72
map[int]struct{[]byte}	        48
map[int]*struct{[100]byte}	    45
map[string]struct{[100]byte}	  42
map[[10]byte]struct{[100]byte}	4.7
map[int]struct{[100]byte}	      4.6
```
