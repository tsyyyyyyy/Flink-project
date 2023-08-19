package project.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import project.POJO.Nation;


public class NationStateProcess extends KeyedProcessFunction<Integer, Nation, Tuple3<Boolean, Integer, String>> {
    private String name;
    private ValueState<Nation> nationValueState;

    public NationStateProcess(String name) {
        this.name = name;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        nationValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Nation>("nation", Nation.class)
        );
    }

    @Override
    public void processElement(Nation nation, KeyedProcessFunction<Integer, Nation, Tuple3<Boolean, Integer, String>>.Context ctx, Collector<Tuple3<Boolean, Integer, String>> out) throws Exception {
        if ((nationValueState.value() == null && nation.active) || (nationValueState != null && !nation.active)) {
            if (!nation.active) {//update state if supplier is active
                nationValueState.clear();
            } else {
                nationValueState.update(nation);
            }
        }
        if (nation.name.equals(this.name)) {
            //System.out.println("Nation");
            Tuple3<Boolean, Integer, String> tmpResult = Tuple3.of(nation.active, nation.nationKey, nation.name);
            out.collect(tmpResult);
        }
    }
}
