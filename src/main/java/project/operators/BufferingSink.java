package project.operators;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class BufferingSink implements SinkFunction<Tuple2<Integer, BigDecimal>>, CheckpointedFunction {

    //定义当前类的属性
    private final int threshold;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    // 必须作为算子状态保存起来，才能持久化到磁盘，写入到检查点中
    private List<Tuple2<Integer, BigDecimal>> bufferedElements;
    // 定义一个算子状态
    private ListState<Tuple2<Integer, BigDecimal>> checkpointedState;


    @Override
    public void invoke(Tuple2<Integer, BigDecimal> value, Context context) throws Exception {
        bufferedElements.add(value); // 缓存到列表
        // 判断如果达到阈值就批量写入
        if (bufferedElements.size() == threshold) {
            // 用打印到控制台模拟写入外部系统
            for (Tuple2<Integer, BigDecimal> element : bufferedElements) {
                System.out.println(element);
            }
            System.out.println("==============输出完毕==============");
            bufferedElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // 清空状态
        checkpointedState.clear();

        // 对状态进行持久化,复制缓存的列表 到 列表状态
        for (Tuple2<Integer, BigDecimal> bufferedElement : bufferedElements) {
            checkpointedState.add(bufferedElement);
        }

    }


    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // 定义算子状态
        ListStateDescriptor<Tuple2<Integer, BigDecimal>> eventListStateDescriptor = new ListStateDescriptor<>("buffered-elements", Types.TUPLE(Types.INT, Types.BIG_DEC));
        checkpointedState = functionInitializationContext.getOperatorStateStore().getListState(eventListStateDescriptor);
        // 如果从故障恢复，需要将ListState中所有元素复制到 本地列表中
        if (functionInitializationContext.isRestored()) {
            for (Tuple2<Integer, BigDecimal> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}