package project;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import project.DataSource.DataSource;
import project.POJO.Nation;
import project.POJO.Partsupp;
import project.POJO.Supplier;
import project.operators.*;

import java.math.BigDecimal;
import org.apache.flink.core.fs.Path;
import java.util.concurrent.TimeUnit;


public class Main {
    // Define output tag
    private static final OutputTag<Nation> nationOutputTag = new OutputTag<Nation>("nation"){};
    private static final OutputTag<Supplier> supplierOutputTag = new OutputTag<Supplier>("supplier"){};
    private static final OutputTag<Partsupp> partsuppOutputTag = new OutputTag<Partsupp>("partsupp"){};

    public static void main(String[] args) throws Exception {
        // Set query condition
        float fraction = 0.0001F;
        String name = "GERMANY";
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024*1024*1024)
                                //文件大小已达到 1 GB
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(15))
                                //至少包含 15 分钟的数据
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(5))
                                // 最近 5 分钟没有收到新的数据
                                // 然后就可以滚动数据
                                .build()
                )
                .build();
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Read data from file.
        String filepath = "data/data 100M/input.txt";
        DataStreamSource<String> customStream = env.addSource(new DataSource(filepath));

        // Split data into three streams.
        SingleOutputStreamOperator<String> dataStream = customStream.process(new SpiltStream());
        DataStream<Nation> nationDataStream = dataStream.getSideOutput(nationOutputTag);
        DataStream<Partsupp> partsuppDataStream = dataStream.getSideOutput(partsuppOutputTag);
        DataStream<Supplier> supplierDataStream = dataStream.getSideOutput(supplierOutputTag);

        env.setParallelism(8);

        // Filter nation stream
        SingleOutputStreamOperator<Tuple3<Boolean, Integer, String>> processedNationStream =
                nationDataStream.keyBy(nation -> nation.nationKey).process(new NationStateProcess(name));

        SingleOutputStreamOperator<Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String>> processedStream =
                partsuppDataStream.connect(supplierDataStream)
                .keyBy(partsupp -> partsupp.suppKey, supplier -> supplier.suppKey)
                .process(new PartsuppMatchSupp())
                .connect(processedNationStream)
                .keyBy(t1 -> t1.f3, t2 -> t2.f1).process(new TmpMatchNation());

        // Calculate sum
        SingleOutputStreamOperator<Tuple4<Boolean, Integer, Integer, BigDecimal>> processSum =
                processedStream.keyBy(data -> data.f3).process(new CalculateSum(fraction));
        processSum.print("nation sum");
        processedStream.keyBy(data -> data.f1).process(new CalculateSum()).print("part sum");
        processedStream.keyBy(data -> data.f1).process(new CalculateSum()).connect(processSum)
                .keyBy(t1 -> t1.f1, t2->t2.f1).process(new JoinSubTable()).keyBy(data -> data.f2).process(new SortResult()).print();

        //processedStream.keyBy(data -> data.f1).process(new CalculateSum()).connect(processSum)
        //        .keyBy(t1 -> t1.f1, t2->t2.f1).process(new JoinSubTable()).keyBy(data -> data.f2).process(new SortResult()).map(data -> data.f0+ " " + data.f1)
        //        .addSink(streamingFileSink);

        env.execute();
    }

    public static class SpiltStream extends ProcessFunction<String, String> {
        @Override
        public void processElement(String line, Context ctx, Collector<String> out) throws Exception {
            String[] fields = line.split("\t");
            //System.out.println(line);
            //System.out.println(fields);
            String type = fields[1];
            //System.out.println(type);

            if (type.equals("nation")) {
                ctx.output(nationOutputTag, new Nation(fields));
            } else if (type.equals("partsupp")) {
                ctx.output(partsuppOutputTag, new Partsupp(fields));
            } else if (type.equals("supplier")) {
                ctx.output(supplierOutputTag, new Supplier(fields));
            }

        }
    }

}
