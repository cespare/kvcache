## Initial benchmarking results

Very rough benchmark; just doing serial requests with random data and full read/write locking.

* DEFLATE is very slow. Just using compress/flate with `DefaultCompression` ran about 1300 qps; whereas no
  compression was about 5000 qps.
* Snappy is a lot faster; about 4000 qps.
