package com.kapild;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Utils {
    private static final String HDFS_FILE_PREFIX = "part-";

    public static List<Path> getInputPaths(Configuration conf,
            String hiveWarehousePath, String tableName) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(hiveWarehousePath + "/" + tableName + "/*");
        List<Path> inputPaths = new ArrayList<Path>();
        for (FileStatus fileStatus : fs.globStatus(path)) {
            inputPaths.add(fileStatus.getPath());
        }
        return inputPaths;
    }

    public static List<Path> getInputPaths(Configuration conf,
            String parentDirectory) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(parentDirectory + "/" + HDFS_FILE_PREFIX + "*");
        List<Path> inputPaths = new ArrayList<Path>();
        for (FileStatus fileStatus : fs.globStatus(path)) {
            inputPaths.add(fileStatus.getPath());
        }
        return inputPaths;
    }
}
