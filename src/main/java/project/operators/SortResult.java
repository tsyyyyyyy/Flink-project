package project.operators;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;

public class SortResult extends KeyedProcessFunction<Integer, Tuple5<Boolean, Integer, Integer, BigDecimal, BigDecimal>,
        Tuple2<Integer, BigDecimal>>{

    private MapState<Integer, BigDecimal> partSuppListState;
    @Override
    public void open(Configuration parameters) throws Exception {
        partSuppListState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, BigDecimal>("result", Types.INT, Types.BIG_DEC));
    }

    @Override
    public void processElement(Tuple5<Boolean, Integer, Integer, BigDecimal, BigDecimal> tuple, KeyedProcessFunction<Integer, Tuple5<Boolean, Integer, Integer, BigDecimal, BigDecimal>, Tuple2<Integer, BigDecimal>>.Context ctx, Collector<Tuple2<Integer, BigDecimal>> out) throws Exception {

        //System.out.println("======================");

        if (tuple.f0) {
            partSuppListState.put(tuple.f1, tuple.f3);
        } else {
            if (partSuppListState.contains(tuple.f1)) {
                partSuppListState.remove(tuple.f1);
            }
        }

        ArrayList<Tuple2<Integer, BigDecimal>> result = new ArrayList<>();
        ArrayList<Integer> removal = new ArrayList<>();
        for (Integer key : partSuppListState.keys()) {
            if (partSuppListState.get(key).compareTo(tuple.f4) < 0) {
                //partSuppListState.remove(key);
                removal.add(key);
            } else {
                result.add(Tuple2.of(key, partSuppListState.get(key)));
            }
        }
        for (Integer key : removal) {
            partSuppListState.remove(key);
        }

        result.sort(new Comparator<Tuple2<Integer, BigDecimal>>() {
            @Override
            public int compare(Tuple2<Integer, BigDecimal> o1, Tuple2<Integer, BigDecimal> o2) {
                return o2.f1.compareTo(o1.f1);
            }
        });

        for (Tuple2<Integer, BigDecimal> t : result) {
            out.collect(t);
        }

        result.clear();
        removal.clear();
        /*Integer output_size = result.size() > 50 ? 50 : result.size();
        for (Tuple2<Integer, BigDecimal> t : result) {
            if (output_size > 0) {
                out.collect(t);
                output_size -= 1;
            } else {
                break;
            }
        }*/
    }
}
