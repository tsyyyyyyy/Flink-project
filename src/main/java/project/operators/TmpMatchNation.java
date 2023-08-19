package project.operators;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

public class TmpMatchNation extends CoProcessFunction<Tuple6<Boolean, Integer, Integer, Integer, Integer, BigDecimal>,
        Tuple3<Boolean, Integer, String>, Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String>> {
    private ListState<Tuple6<Boolean, Integer, Integer, Integer, Integer, BigDecimal>> tmpListState;
    private ValueState<Tuple3<Boolean, Integer, String>> nationValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        tmpListState= getRuntimeContext().getListState(
                new ListStateDescriptor<Tuple6<Boolean, Integer, Integer, Integer, Integer, BigDecimal>>("tmp",
                        Types.TUPLE(Types.BOOLEAN, Types.INT, Types.INT, Types.INT, Types.INT, Types.BIG_DEC))
        );

        nationValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Tuple3<Boolean, Integer, String>>("nation",
                        Types.TUPLE(Types.BOOLEAN, Types.INT, Types.STRING))
        );
    }

    @Override
    public void processElement1(Tuple6<Boolean, Integer, Integer, Integer, Integer, BigDecimal> tmp,
                                CoProcessFunction<Tuple6<Boolean, Integer, Integer, Integer, Integer, BigDecimal>,
                                        Tuple3<Boolean, Integer, String>, Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String>>.Context ctx,
                                Collector<Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String>> out) throws Exception {
        tmpListState.add(tmp);
        if (nationValueState.value() != null) {
            boolean active = tmp.f0 && nationValueState.value().f0;
            int symbol = active ? 1 : -1; //if active then + else -
            if (nationValueState.value().f0 || tmp.f0) {
                Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String> output =
                        Tuple6.of(active, tmp.f1, tmp.f2, tmp.f3, tmp.f5.multiply(BigDecimal.valueOf(tmp.f4 * symbol)),
                                nationValueState.value().f2);
                out.collect(output);
            }
        }
    }

    @Override
    public void processElement2(Tuple3<Boolean, Integer, String> nation,
                                CoProcessFunction<Tuple6<Boolean, Integer, Integer, Integer, Integer, BigDecimal>,
                                        Tuple3<Boolean, Integer, String>, Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String>>.Context ctx,
                                Collector<Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String>> out) throws Exception {
        if ((nationValueState.value() == null && nation.f0) || (nationValueState != null && !nation.f0)) {
            if (!nation.f0) {//update state if supplier is active
                nationValueState.clear();
            } else {
                nationValueState.update(nation);
            }

            for (Tuple6<Boolean, Integer, Integer, Integer, Integer, BigDecimal> tmp : tmpListState.get()) {
                boolean active = tmp.f0 && nation.f0;
                int symbol = active ? 1 : -1; //if active then + else -
                Tuple6<Boolean, Integer, Integer, Integer, BigDecimal, String> output =
                        Tuple6.of(active, tmp.f1, tmp.f2, tmp.f3, tmp.f5.multiply(BigDecimal.valueOf(tmp.f4 * symbol)),
                                nationValueState.value().f2);
                if (tmp.f0 || nation.f0) {
                    out.collect(output);
                }
            }
        }


    }
}
