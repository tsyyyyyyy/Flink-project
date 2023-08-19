package project.POJO;

import java.math.BigDecimal;

public class Supplier {
    public Integer suppKey;
    public String name;
    public String address;
    public Integer nationKey;
    public String phone;
    public BigDecimal acctBal;
    public String comment;
    public Boolean active;
    public String type;

    //constructor
    public Supplier(Boolean active, String type, Integer suppKey, String name, String address, Integer nationKey,
                    String phone, BigDecimal acctBal, String comment) {
        this.active = active;
        this.type = type;
        this.suppKey = suppKey;
        this.name = name;
        this.address = address;
        this.nationKey = nationKey;
        this.phone = phone;
        this.acctBal = acctBal;
        this.comment = comment;
    }

    public Supplier(String[] fields) {
        this.active = fields[0].equals("+");;
        this.type = fields[1];
        this.suppKey = Integer.parseInt(fields[2]);
        this.name = fields[3];
        this.address = fields[4];
        this.nationKey = Integer.parseInt(fields[5]);
        this.phone = fields[6];
        this.acctBal = new BigDecimal(fields[7]);
        this.comment = fields[8];
    }

    @Override
    public String toString() {
        return "Supplier{" +
                "suppKey=" + suppKey +
                ", name='" + name + '\'' +
                ", address='" + address + '\'' +
                ", nationKey=" + nationKey +
                ", phone='" + phone + '\'' +
                ", acctBal=" + acctBal +
                ", comment='" + comment + '\'' +
                ", active=" + active +
                ", type='" + type + '\'' +
                '}';
    }
}
