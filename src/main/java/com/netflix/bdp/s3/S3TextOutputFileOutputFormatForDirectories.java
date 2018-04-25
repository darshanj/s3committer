package com.netflix.bdp.s3;

import com.netflix.bdp.s3.S3DirectoryOutputCommitter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class S3TextOutputFileOutputFormatForDirectories extends org.apache.hadoop.mapreduce.lib.output.TextOutputFormat {
    private S3DirectoryOutputCommitter committer = null;

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new S3DirectoryOutputCommitter(output,context);
        }
        return committer;
    }
}
