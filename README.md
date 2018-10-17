# Content Reuse Detection

This repository contains the code and pointers to datasets used in the
paper "Precise Detection of Content Reuse in the Web" by Calvin Ardi and
John Heidemann.

`data/` contains pointers to datasets used in the paper, along with
lists of files for verification. The data can usually be accessed on
Amazon's S3 or downloaded via HTTP or BitTorrent.

`java/` contains code using Apache Hadoop 1.x MapReduce to generate hashes
of files and their corresponding chunks. For easier processing on files,
it's advisable to convert archives (`.tar`, etc.) to [SequenceFile]s
(`.seq`) using something like [forqlift].

`run_scripts/` contains shell scripts used for executing the `.jar`
generated in `java/`. Since most of the Java MapReduce code was run on
Amazon's EMR service, it also contains scripts to create/destroy
instances.

[SequenceFile]: https://wiki.apache.org/hadoop/SequenceFile
[forqlift]: http://www.exmachinatech.net/projects/forqlift/

`python/` contains code using Apache Hadoop 1.x Streaming and requires
[mrjob]. Code here handles the processing of the intermediate output
from the Java code to generate the final outputs.

[mrjob]: https://pypi.org/project/mrjob/

## Other Notes

At the time of writing, we ultimately found that Hadoop MapReduce
performed best on binary and large archives using Java and
SequenceFiles.

The intermediate output could then be efficiently processed using Hadoop
Streaming in the language of choice (in our case, Python).
