package com.datinko.prototype.bigdata.kafka.producer;

import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;

public class BufferedFileReader implements Runnable {

    private final Logger LOGGER = Logger.getLogger(BufferedFileReader.class);
    private final Charset ENC = Charset.forName("UTF-8");
    private OutputStream outputStream = null;
    private String fileToRead;

    public BufferedFileReader(String file, OutputStream stream) {

        this.fileToRead = file;
        this.outputStream = stream;
    }

    public void run() {

        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            try {
                reader = new BufferedReader(new FileReader(this.fileToRead));
                writer = new BufferedWriter(new OutputStreamWriter(
                        this.outputStream, ENC));
                int b = -1;
                LOGGER.debug("Reading stream");
                while ((b = reader.read()) != -1) {
                    writer.write(b);
                }
                LOGGER.debug("Finished reading");
            } catch (FileNotFoundException ex) {
                LOGGER.fatal("Tried to read a file that does not exist", ex);
                LOGGER.trace("", ex);
            } catch (IOException ex) {
                LOGGER.fatal("IO Error while reading file", ex);
                LOGGER.trace("", ex);
            } finally {
                try {
                    if (reader != null) reader.close();
                    if (writer != null) writer.close();
                } catch (IOException ex) {
                    LOGGER.fatal("IO Error while cleaning up", ex);
                    LOGGER.trace(null, ex);
                }
            }
        } catch (Exception ex) {
            LOGGER.fatal("Error while reading file", ex);
            LOGGER.trace("", ex);
        }
    }
}
