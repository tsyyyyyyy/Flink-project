package project.operators;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import project.POJO.Partsupp;
import project.POJO.Supplier;

import java.math.BigDecimal;

public class PartsuppMatchSupp extends CoProcessFunction<Partsupp, Supplier, Tuple6<Boolean, Integer, Integer,
        Integer, Integer, BigDecimal>> {

    private ListState<Partsupp> partsuppListState;
    private ValueState<Supplier> supplierValueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        partsuppListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Partsupp>("part_supp", Partsupp.class)
        );
        supplierValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Supplier>("supplier", Supplier.class)
        );
    }

    @Override
    public void processElement1(Partsupp partsupp, CoProcessFunction<Partsupp, Supplier, Tuple6<Boolean,
            Integer, Integer, Integer, Integer, BigDecimal>>.Context ctx, Collector<Tuple6<Boolean, Integer,
            Integer, Integer, Integer, BigDecimal>> out) throws Exception {
        boolean currentState = partsupp.active;
        //if current partsupp is not active, then check if previous added partsupp that related to the current supplier
        //contains any insertion (active partsupp) or not. If there is no active partsupp, then there is no need to
        //join two tables for the current supplier(no match for the current key), otherwise join.
        if (!currentState) {
            // check if there is any active partsupp record or not
            int insertion = 0;
            for (Partsupp partsupp1 : partsuppListState.get()) {
                if (partsupp1.active) {
                    insertion += 1;
                } else {
                    insertion -= 1;
                }
            }
            if (insertion > 0) {
                currentState = true;
            }
        }
        //no active partsupp record, return
        if (!currentState) {
            return;
        }
        partsuppListState.add(partsupp);
        Supplier supplier = supplierValueState.value();
        if (supplier != null) {
            boolean active = supplier.active && partsupp.active;
            Tuple6<Boolean, Integer, Integer, Integer, Integer, BigDecimal> tmpResult =
                    Tuple6.of(active, partsupp.partKey, supplier.suppKey, supplier.nationKey,
                            partsupp.availqty, partsupp.supplyCost);

            if (partsupp.active || supplier.active) {
                //System.out.println("Partsupp");
                out.collect(tmpResult);
            }
        }
    }
    

    @Override
    public void processElement2(Supplier supplier, CoProcessFunction<Partsupp, Supplier, Tuple6<Boolean,
            Integer, Integer, Integer, Integer, BigDecimal>>.Context ctx, Collector<Tuple6<Boolean, Integer,
            Integer, Integer, Integer, BigDecimal>> out) throws Exception {
        if ((supplierValueState.value() == null && supplier.active) ||
                (supplierValueState.value() != null && !supplier.active)) {
            if (!supplier.active) {//update state if supplier is active
                supplierValueState.clear();
            } else {
                supplierValueState.update(supplier);
            }
            for (Partsupp partsupp: partsuppListState.get()) {
                boolean active = partsupp.active && supplier.active;
                Tuple6<Boolean, Integer, Integer, Integer, Integer, BigDecimal> tmpResult =
                        Tuple6.of(active, partsupp.partKey, supplier.suppKey, supplier.nationKey,
                                partsupp.availqty, partsupp.supplyCost);

                if (partsupp.active || supplier.active) {
                    //System.out.println("Supplier");
                    out.collect(tmpResult);
                }
            }
        }
    }

}
