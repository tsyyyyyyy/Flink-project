package project.POJO;

import java.math.BigDecimal;
import java.util.Arrays;

public class Partsupp {
    public Integer partKey;
    public Integer suppKey;
    public Integer availqty;
    public BigDecimal supplyCost;
    public String comment;
    public Boolean active;
    public String type;
    //constructor
    public Partsupp() {
    }

    @Override
    public String toString() {
        return "Partsupp{" +
                "partKey=" + partKey +
                ", suppKey=" + suppKey +
                ", availqty=" + availqty +
                ", supplyCost=" + supplyCost +
                ", comment='" + comment + '\'' +
                ", active=" + active +
                ", type='" + type + '\'' +
                '}';
    }

    public Partsupp(Boolean active, String type, Integer partKey, Integer suppKey, Integer availqty,
                    BigDecimal supplyCost, String comment) {
        this.active = active;
        this.type = type;
        this.partKey = partKey;
        this.suppKey = suppKey;
        this.availqty = availqty;
        this.supplyCost = supplyCost;
        this.comment = comment;
    }

    public Partsupp(String[] fields) {
        this.active = fields[0].equals("+");;
        this.type = fields[1];
        this.partKey = Integer.parseInt(fields[2]);
        this.suppKey = Integer.parseInt(fields[3]);
        this.availqty = Integer.parseInt(fields[4]);
        this.supplyCost = new BigDecimal(fields[5]);
        this.comment = fields[6];
    }

}
