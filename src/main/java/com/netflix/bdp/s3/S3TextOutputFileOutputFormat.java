package com.netflix.bdp.s3;

import com.netflix.bdp.s3.S3PartitionedOutputCommitter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class S3TextOutputFileOutputFormat extends org.apache.hadoop.mapreduce.lib.output.TextOutputFormat {
    private S3PartitionedOutputCommitter committer = null;

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new S3PartitionedOutputCommitter(output,context);
        }
        return committer;
    }
}