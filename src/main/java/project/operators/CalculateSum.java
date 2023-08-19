package project.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

public class CalculateSum extends KeyedProcessFunction<Integer, Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String>, Tuple4<Boolean, Integer, Integer, BigDecimal>> {
    private float fraction;
    private ValueState<BigDecimal> sumState;
    public CalculateSum(float fraction) {
        this.fraction = fraction;
    }

    public CalculateSum() {
        this.fraction = 1F;
    }

    @Override
    public void processElement(Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String> tuple, KeyedProcessFunction<Integer, Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String>, Tuple4<Boolean, Integer, Integer, BigDecimal>>.Context ctx, Collector<Tuple4<Boolean, Integer, Integer, BigDecimal>> out) throws Exception {
        BigDecimal prevSum = sumState.value();
        BigDecimal sum = prevSum != null ? prevSum : BigDecimal.valueOf(0);

//        if (!tuple.f0) {
//            out.collect(Tuple3.of(false, tuple.f3, sum));
//        }
        sum = sum.add(tuple.f4.multiply(BigDecimal.valueOf(fraction)));
        if (sum.compareTo(BigDecimal.valueOf(0)) > 0) {
            sumState.update(sum);
            out.collect(Tuple4.of(true, tuple.f1, tuple.f3, sum));
        } else if (sum.compareTo(BigDecimal.valueOf(0)) == 0) {
            sumState.update(sum);
            out.collect(Tuple4.of(false, tuple.f1, tuple.f3, sum));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sumState = getRuntimeContext().getState(
                new ValueStateDescriptor<BigDecimal>("sumState", Types.BIG_DEC)
        );
    }


}
