#perf-framework
==============

This will allow running benchmarking jobs.

Currently, the benchmark will allow performing the following tasks:
1. Run benchmark jobs with configurable benchmark and platforms
2. Validate the output for failure
3. Record the benchmarking results in a CSV file


###Installing the gem
After cloning the repository, cd into the directory, and run rake clean all to install the perf_framework gem.
```bash
$ cd perf_framework
$ rake clean all
```
###Running the application
```bash
$ perf --help
Usage: perf [options]
    -b BENCHMARK_JSON_PATH,          path to benchmark.json
        --benchmark_path
    -e EMR_LAUNCH_JSON_PATH,         path to emr-launch-config.json
        --emr-launch-config
    -k, --keep-alive                 flag to keep the emr instance alive
    -p PLATFORM_JSON_PATH,           path to platform.json
        --platform_name
    -l, --log-level LEVEL            Log level: debug, info, warn, error, fatal
    -o, --output FILE                output file to write the result
    -u, --uniquify                   uniquify output
    -v, --version                    version of the tool
    -j, --job_label LABEL_NAME       A label for the job
    -h, --help                       Show this help.
```
Options for running the benchmark:

-b: Path to a benchmark.json file which lists options for the map-reduce run such as input/output, and the location of the map-reduce jar. [Here](https://github.com/Altiscale/perf-framework/blob/master/spec/lib/resources/wikilogs-config.json) is an example.

-p: Path to a platform.json file which lists options for the particular platform such as host name of the master, username, ssh-key. [Here](https://github.com/Altiscale/perf-framework/blob/master/spec/lib/resources/emr-config.json) is an example.

-u: Flag that the output folder path should be uniquified using a timestamp.

-l: Set the logging level using this.

-o: A csv file where the tool is going to record a summary of the run.

-j: An optional label on the results csv file (usually used to identify a run among many).

-k: This flag indicates that the emr cluster should be kept alive after the job is complete. Without this flag, the emr cluster would terminate after running the job.

-e: You can provide an emr-launch-config.json, which will indicate how to start an emr cluster. See the [AWS::EMR::Client documentation](http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/EMR/Client.html#run_job_flow-instance_method) for more information.

###Benchmark and platform json
####benchmark.json

This file configures the benchmark run. It provides the input, output, location of the hadoop_jar to run, and the main class to run.
```json
{
    "benchmark": "<benchmark_name>",
    "platformspec": {
        "<platform_name>": {
            "cleanup_command": "<a cleanup command to cleanup the output dir>",
            "hadoop_jar": "<path/to/hadoop/jar/on/cluster>",
            "input": "<hdfs or s3 path to input>",
            "main_class": "<main java class of the hadoop jar>",
            "output": "<hdfs or s3 path to output>",
            "post_transfers": [
                {
                    "from": "<hdfs or s3 from directory>",
                    "to": "<hdfs or s3 to directory>"
                }
            ],
            "pre_transfers": [
                {
                    "scp": "<true to indicate that this would be a scp copy>",
                    "from": "<local directory>",
                    "to": "<directory on cluster host (emr host or desktop)>"
                },
                {
                    "from": "<hdfs or s3 from directory>",
                    "to": "<hdfs or s3 to directory>"
                }
            ],
            "run_options": "<run options for the hadoop jar>"
        }
    }
}
```

####platform.json

Put the hostname from the Job Flow description (from elastic-mapreduce --list above) into the platform configuration file.  Also be sure to put the instance type and number of slaves into the configuration, so that your benchmark results will be automatically annotated with this information:
```json
{
 "platform":"<platform_name>",
 "jobflow_id":"<the emr job flow>",
 "host":"<host name of the emr master>",
 "user":"hadoop",
 "ssh_key":"<ssh_key>",
 "node_type":"m1.large",
 "hadoop_slaves":10
}
```

###Running the benchmark
You can run the benchmark by providing a platform configuration file and a benchmark configuration file. In addition, you can change the default logging to debug, change the default output file, provide a custom label to identify your run, and copy the job history files to s3.
An example run (for emr):
```bash
$ perf -b ~/dev/perf/perf-configs/teragen-config.json -p ~/dev/perf/perf-configs/emr-config.json -l debug -o ~/dev/perf/my_perf_results.csv -j teragen_large
```
###Optional: Launching and terminating emr with the benchmark

If you want to launch (and terminate) an emr cluster from the perf-framework, you can provide an emr-launch-config.json to the -e flag mentioned above. See aws documentation on the structure of this json file.
Alternatively, you can provide steps to run a job in the emr-launch-config.json. In that case you can omit providing a benchmark.json (since emr-launch-config.json will contain the benchmark information).
