package project.DataSource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Data Source
 */
public class DataSource implements SourceFunction<String> {
    private final String filename;

    public DataSource(String filename) {
        this.filename = filename;
    }

    public void run(SourceContext<String> ctx) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line = reader.readLine();
            while (line != null) {
                if (!line.equals("End") || !line.equals("")) {
                    ctx.collect(line);
                }
                line = reader.readLine();
            }
        }
    }

    @Override
    public void cancel() {
    }
}


