# s3bench

The s3bench utility is a simple tool to test s3 server performance.

```
# export AWS_SECRET_ACCESS_KEY=user
# export AWS_ACCESS_KEY_ID=password
# ./s3bench -upload -bucket test -endpoint http://s3-server.example.com:12345 -objectsize 10737418240  -pathstyle -prefix host1/file. -region us-east-1 -concurrency 8 -disablechecksum -n 4
0: 10737418240 in 11.723064482s (873 MB/s)
1: 10737418240 in 11.536811023s (887 MB/s)
2: 10737418240 in 11.639824457s (879 MB/s)
3: 10737418240 in 11.67056153s (877 MB/s)
 
run perf: 42949672960 in 11.729466731s (3492 MB/s)

# ./s3bench -download -bucket test -endpoint http://s3-server.example.com:12345 -objectsize 10737418240  -pathstyle -prefix host1/file. -region us-east-1 -concurrency 8 -disablechecksum -n 4
0: 10737418240 in 6.92038851s (1479 MB/s)
1: 10737418240 in 6.928683753s (1477 MB/s)
2: 10737418240 in 6.953225327s (1472 MB/s)
3: 10737418240 in 6.952238442s (1472 MB/s)
 
: run perf: 42949672960 in 6.954156319s (5890 MB/s)
```

useful options:

* endpoint is the server listening endpoint, use https for ssl connections
* chunksize is the part size for the multipart upload
* concurrency is the max simultaneous part uploads in multipart upload or range gets for download
* n is the number of objects to read/write simultaneously
* prefix it prepended to each object, object name ends in unique number
* rand uses randomly generated data instead of all 0s
* objectsize is how much total data to write per object
* disablechecksum sets x-amz-content-sha256 header to UNSIGNED-PAYLOAD instead of calculating payload hash values
* debug enables extra debugging output

The upload/download option specifies if this is an upload or download test.  The download object must exist already, so it is suggest to start with upload then follow with download.

When downloading, data is retrieved from the server and then discarded so that there is no artifical bottleneck from local storage.

Required options are the standard -access -secret -region -bucket -endpoint for any s3 session.  Requires read/write permissions ot the bucket depending on test.