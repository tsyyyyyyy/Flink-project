package project.operators;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

public class JoinSubTable extends CoProcessFunction<Tuple4<Boolean, Integer, Integer, BigDecimal>, Tuple4<Boolean, Integer, Integer, BigDecimal>, Tuple5<Boolean, Integer, Integer, BigDecimal, BigDecimal>> {
    private ValueState<Tuple4<Boolean, Integer, Integer, BigDecimal>> partsuppState;
    private ValueState<Tuple4<Boolean, Integer, Integer, BigDecimal>> nationValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        partsuppState = getRuntimeContext().getState(
                new ValueStateDescriptor<Tuple4<Boolean, Integer, Integer, BigDecimal>>("tmp",
                        Types.TUPLE(Types.BOOLEAN, Types.INT, Types.INT, Types.BIG_DEC))
        );

        nationValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Tuple4<Boolean, Integer, Integer, BigDecimal>>("nation",
                        Types.TUPLE(Types.BOOLEAN, Types.INT, Types.INT, Types.BIG_DEC))
        );
    }

    @Override
    public void processElement1(Tuple4<Boolean, Integer, Integer, BigDecimal> part, CoProcessFunction<Tuple4<Boolean, Integer, Integer, BigDecimal>, Tuple4<Boolean, Integer, Integer, BigDecimal>, Tuple5<Boolean, Integer, Integer, BigDecimal, BigDecimal>>.Context ctx, Collector<Tuple5<Boolean, Integer, Integer, BigDecimal, BigDecimal>> out) throws Exception {
        //System.out.println("=================part update====================");
        //System.out.println(part);
        //update state for the particular part
        partsuppState.update(part);
        if (nationValueState.value() != null) {
            if (part.f3.compareTo(nationValueState.value().f3) > 0) {
                out.collect(Tuple5.of(true, part.f1, part.f2, part.f3, nationValueState.value().f3));
            } else {
                out.collect(Tuple5.of(false, part.f1, part.f2, part.f3, nationValueState.value().f3));
            }
        }
    }

    @Override
    public void processElement2(Tuple4<Boolean, Integer, Integer, BigDecimal> nation, CoProcessFunction<Tuple4<Boolean, Integer, Integer, BigDecimal>, Tuple4<Boolean, Integer, Integer, BigDecimal>, Tuple5<Boolean, Integer, Integer, BigDecimal, BigDecimal>>.Context ctx, Collector<Tuple5<Boolean, Integer, Integer, BigDecimal, BigDecimal>> out) throws Exception {
        //System.out.println("=================nation update====================");
        //System.out.println(nation);
        if (partsuppState.value() == null) {
            //do nothing
        } else {
            if (partsuppState.value().f3.compareTo(nation.f3) > 0) {
                out.collect(Tuple5.of(true, partsuppState.value().f1, partsuppState.value().f2,
                        partsuppState.value().f3, nation.f3));
            } else {
                out.collect(Tuple5.of(false, partsuppState.value().f1, partsuppState.value().f2,
                        partsuppState.value().f3, nation.f3));
            }
        }
        nationValueState.update(nation);
    }
}
