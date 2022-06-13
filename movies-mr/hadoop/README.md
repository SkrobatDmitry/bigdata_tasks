# Local
---
The script is used for searching movies in the CSV-format file by the following parameters: genre, release year, title matches. [Hadoop Streaming](https://hadoop.apache.org/docs/r1.2.1/streaming.html) is used to process the dataset.

## Usage
---
```text
usage: get-movies.sh [-h] [--N amount] [--genres genres] [--year-from year] [--year-to year] [--regexp regexp]
```

### Parameters
---
- `-h, --help` - show help message and exit.
- `--N amount` - number of movies of each genre to output.
- `--genres genres` - requested movies genres.
- `--year-from year` - the first filter by the year the movie was made.
- `--year-to year` - the second filter by the year the movie was made.
- `--regxep regexp` - filter of movies title or their parts.

### Help message
---
Use `-h` or `--help` to get the help message. For example:
```bash
bash hadoop/get-movies.sh --help
```

The script produces the following output:
```text
usage: get-movies.sh [-h] [--N amount] [--genres genres] [--year-from year] [--year-to year] [--regexp regexp]

optional arguments:
  -h, --help        show this help message and exit

  --N amount        number of movies of each rating to output
  --genres genres   requested movies genres
  --year-from year  the first filter by the year the movie was made
  --year-to year    the second filter by the year the movie was made
  --regexp regexp   filter of movies title or their parts
```

### Number
---
Use the `--N` argument to specify how many movies for each genre to show. For example:
```bash
bash hadoop/setup_envinronment.sh
bash hadoop/get-movies.sh --N 1
hdfs dfs -cat /movies-mr/output/*
```

In this case, the script displays one movie for each genre:
```text
Western,The Beguiled,2017
Animation,Bungo Stray Dogs: Dead Apple,2018
Comedy,Ant-Man and the Wasp,2018
Crime,BlacKkKlansman,2018
Children,A Wrinkle in Time,2018
IMAX,Star Wars: Episode VII - The Force Awakens,2015
Sci-Fi,A Wrinkle in Time,2018
Drama,A Quiet Place,2018
Musical,Strange Magic,2015
Fantasy,A Wrinkle in Time,2018
Mystery,Annihilation,2018
Adventure,A Wrinkle in Time,2018
Documentary,Spiral,2018
Romance,Mamma Mia: Here We Go Again!,2018
Thriller,A Quiet Place,2018
Action,Ant-Man and the Wasp,2018
Film-Noir,Bullet to the Head,2012
Horror,A Quiet Place,2018
War,Darkest Hour,2017
```

### Genres
---
Use the `--genres` argument to specify which genres movies should be displayed in. For example:
```bash
bash hadoop/setup_envinronment.sh
bash hadoop/get-movies.sh --N 2 --genres "Crime|IMAX|Thriller"
hdfs dfs -cat /movies-mr/output/*
```

In this case, the script displays two movies for each requested genre:
```text
Crime,BlacKkKlansman,2018
Crime,Death Wish,2018
IMAX,Star Wars: Episode VII - The Force Awakens,2015
IMAX,300: Rise of an Empire,2014
Thriller,A Quiet Place,2018
Thriller,Alpha,2018
```

### Year
---
Specify `--year-from` or `--year-to` or both arguments to determine from which to which year the movies will be displayed. For example:
```bash
bash hadoop/setup_envinronment.sh
bash hadoop/get-movies.sh --N 3 --genres "Action|Animation" --year-from 1920 --year-to 1930
hdfs dfs -cat /movies-mr/output/*
```

In this case, the script displays three movies released between 1920 and 1930 for each requested genre:
```text
Animation,Steamboat Willie,1928
Action,All Quiet on the Western Front,1930
Action,Aelita: The Queen of Mars (Aelita),1924
Action,"Thief of Bagdad, The",1924
```

### Regexp
---
Use the `--regexp` argument to determine if a title matches a given regular expression. For example:
```bash
bash hadoop/setup_envinronment.sh
bash hadoop/get-movies.sh --N 1 --regexp "Panda"
hdfs dfs -cat /movies-mr/output/*
```

In this case, the script displays one movie with Panda in the title for each genre:
```text
Animation,Kung Fu Panda 3,2016
Comedy,Kung Fu Panda 2,2011
Children,Kung Fu Panda 2,2011
IMAX,Kung Fu Panda 2,2011
Adventure,Kung Fu Panda 3,2016
Action,Kung Fu Panda 3,2016
```

## Hadoop log message
---
After each execution of the script `bash get-movies.sh`, hadoop outputs the following information:
```text
packageJobJar: [] [/usr/lib/hadoop-mapreduce/hadoop-streaming-2.10.1.jar] /tmp/streamjob4734197513236967185.jar tmpDir=null
22/04/10 16:28:54 INFO client.RMProxy: Connecting to ResourceManager at hadoop-claster-m/10.186.0.14:8032
22/04/10 16:28:54 INFO client.AHSProxy: Connecting to Application History server at hadoop-claster-m/10.186.0.14:10200
22/04/10 16:28:55 INFO client.RMProxy: Connecting to ResourceManager at hadoop-claster-m/10.186.0.14:8032
22/04/10 16:28:55 INFO client.AHSProxy: Connecting to Application History server at hadoop-claster-m/10.186.0.14:10200
22/04/10 16:28:55 INFO mapred.FileInputFormat: Total input files to process : 1
22/04/10 16:28:55 INFO mapreduce.JobSubmitter: number of splits:45
22/04/10 16:28:55 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1649606592481_0006
22/04/10 16:28:56 INFO conf.Configuration: resource-types.xml not found
22/04/10 16:28:56 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
22/04/10 16:28:56 INFO resource.ResourceUtils: Adding resource type - name = memory-mb, units = Mi, type = COUNTABLE
22/04/10 16:28:56 INFO resource.ResourceUtils: Adding resource type - name = vcores, units = , type = COUNTABLE
22/04/10 16:28:56 INFO impl.YarnClientImpl: Submitted application application_1649606592481_0006
22/04/10 16:28:56 INFO mapreduce.Job: The url to track the job: http://hadoop-claster-m:8088/proxy/application_1649606592481_0006/
22/04/10 16:28:56 INFO mapreduce.Job: Running job: job_1649606592481_0006
22/04/10 16:29:03 INFO mapreduce.Job: Job job_1649606592481_0006 running in uber mode : false
22/04/10 16:29:03 INFO mapreduce.Job:  map 0% reduce 0%
22/04/10 16:29:12 INFO mapreduce.Job:  map 2% reduce 0%
22/04/10 16:29:13 INFO mapreduce.Job:  map 7% reduce 0%
22/04/10 16:29:14 INFO mapreduce.Job:  map 24% reduce 0%
22/04/10 16:29:15 INFO mapreduce.Job:  map 33% reduce 0%
22/04/10 16:29:19 INFO mapreduce.Job:  map 38% reduce 0%
22/04/10 16:29:20 INFO mapreduce.Job:  map 40% reduce 0%
22/04/10 16:29:23 INFO mapreduce.Job:  map 44% reduce 0%
22/04/10 16:29:24 INFO mapreduce.Job:  map 58% reduce 0%
22/04/10 16:29:25 INFO mapreduce.Job:  map 67% reduce 0%
22/04/10 16:29:26 INFO mapreduce.Job:  map 69% reduce 0%
22/04/10 16:29:27 INFO mapreduce.Job:  map 73% reduce 0%
22/04/10 16:29:32 INFO mapreduce.Job:  map 76% reduce 0%
22/04/10 16:29:33 INFO mapreduce.Job:  map 80% reduce 0%
22/04/10 16:29:34 INFO mapreduce.Job:  map 100% reduce 0%
22/04/10 16:29:42 INFO mapreduce.Job:  map 100% reduce 27%
22/04/10 16:29:44 INFO mapreduce.Job:  map 100% reduce 40%
22/04/10 16:29:45 INFO mapreduce.Job:  map 100% reduce 60%
22/04/10 16:29:46 INFO mapreduce.Job:  map 100% reduce 87%
22/04/10 16:29:47 INFO mapreduce.Job:  map 100% reduce 100%
22/04/10 16:29:49 INFO mapreduce.Job: Job job_1649606592481_0006 completed successfully
22/04/10 16:29:49 INFO mapreduce.Job: Counters: 51
        File System Counters
                FILE: Number of bytes read=866273
                FILE: Number of bytes written=15100301
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=679335
                HDFS: Number of bytes written=744209
                HDFS: Number of read operations=210
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=45
        Job Counters
                Killed map tasks=1
                Launched map tasks=45
                Launched reduce tasks=15
                Data-local map tasks=24
                Rack-local map tasks=21
                Total time spent by all maps in occupied slots (ms)=1111425
                Total time spent by all reduces in occupied slots (ms)=274020
                Total time spent by all map tasks (ms)=370475
                Total time spent by all reduce tasks (ms)=91340
                Total vcore-milliseconds taken by all map tasks=370475
                Total vcore-milliseconds taken by all reduce tasks=91340
                Total megabyte-milliseconds taken by all map tasks=1138099200
                Total megabyte-milliseconds taken by all reduce tasks=280596480
        Map-Reduce Framework
                Map input records=9743
                Map output records=21898
                Map output bytes=822376
                Map output materialized bytes=870233
                Input split bytes=4680
                Combine input records=0
                Combine output records=0
                Reduce input groups=19
                Reduce shuffle bytes=870233
                Reduce input records=21898
                Reduce output records=21898
                Spilled Records=43796
                Shuffled Maps =675
                Failed Shuffles=0
                Merged Map outputs=675
                GC time elapsed (ms)=11466
                CPU time spent (ms)=92730
                Physical memory (bytes) snapshot=29068668928
                Virtual memory (bytes) snapshot=264511389696
                Total committed heap usage (bytes)=26780106752
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=674655
        File Output Format Counters
                Bytes Written=744209
22/04/10 16:29:49 INFO streaming.StreamJob: Output directory: /movies-mr/output
```

After executing the script, the folder `hdfs dfs -ls /movies-mr/output` contains the following files:
```text
-rw-r--r--   2 skrobat_dima hadoop          0 2022-04-10 16:29 /movies-mr/output/_SUCCESS
-rw-r--r--   2 skrobat_dima hadoop       5550 2022-04-10 16:29 /movies-mr/output/part-00000
-rw-r--r--   2 skrobat_dima hadoop      26222 2022-04-10 16:29 /movies-mr/output/part-00001
-rw-r--r--   2 skrobat_dima hadoop     161385 2022-04-10 16:29 /movies-mr/output/part-00002
-rw-r--r--   2 skrobat_dima hadoop      24358 2022-04-10 16:29 /movies-mr/output/part-00003
-rw-r--r--   2 skrobat_dima hadoop       4998 2022-04-10 16:29 /movies-mr/output/part-00004
-rw-r--r--   2 skrobat_dima hadoop      32639 2022-04-10 16:29 /movies-mr/output/part-00005
-rw-r--r--   2 skrobat_dima hadoop     148215 2022-04-10 16:29 /movies-mr/output/part-00006
-rw-r--r--   2 skrobat_dima hadoop          0 2022-04-10 16:29 /movies-mr/output/part-00007
-rw-r--r--   2 skrobat_dima hadoop      47956 2022-04-10 16:29 /movies-mr/output/part-00008
-rw-r--r--   2 skrobat_dima hadoop      49743 2022-04-10 16:29 /movies-mr/output/part-00009
-rw-r--r--   2 skrobat_dima hadoop      18420 2022-04-10 16:29 /movies-mr/output/part-00010
-rw-r--r--   2 skrobat_dima hadoop      53580 2022-04-10 16:29 /movies-mr/output/part-00011
-rw-r--r--   2 skrobat_dima hadoop          0 2022-04-10 16:29 /movies-mr/output/part-00012
-rw-r--r--   2 skrobat_dima hadoop      62089 2022-04-10 16:29 /movies-mr/output/part-00013
-rw-r--r--   2 skrobat_dima hadoop     109054 2022-04-10 16:29 /movies-mr/output/part-00014
```