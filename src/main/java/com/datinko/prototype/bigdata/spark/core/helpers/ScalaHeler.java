package com.datinko.prototype.bigdata.spark.core.helpers;

import java.util.ArrayList;
import java.util.List;

public class ScalaHeler {

    /**
     * Returns the HDFS URL
     */
    public static String getCheckpointDirectory() {

        String result;

        try {
            List<String> commands = new ArrayList<String>();
            commands.add("bash");
            commands.add("-c");
            commands.add("curl -s http://169.254.169.254/latest/meta-data/hostname");

            SystemCommandExecutor commandExecutor = new SystemCommandExecutor(commands);
            int executionResult = commandExecutor.executeCommand();

            StringBuilder stdout = commandExecutor.getStandardOutputFromCommand();
            StringBuilder stderr = commandExecutor.getStandardErrorFromCommand();

            String name = stdout.toString();

//            System.out.println("STDOUT");
//            System.out.println(stdout);
//
//            System.out.println("STDERR");
//            System.out.println(stderr);

            System.out.println("Hostname = " + name);
            result = "hdfs://" + name.trim() + ":9000/checkpoint/";

            if (stderr.toString().length() > 0 || stdout.toString().length()==0) {
                result = "./checkpoint/";
            }
        } catch (Exception e) {
            result = "./checkpoint/";
        }

        return result;
    }
}
