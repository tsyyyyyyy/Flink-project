package project.POJO;

import java.util.Arrays;

public class Nation {
    public Integer nationKey;
    public String name;
    public Integer regionKey;
    public String comment;
    public Boolean active;
    public String type;

    //constructor
    public Nation() {
    }

    public Nation(Boolean active, String type, Integer nationKey, String name, Integer regionKey, String comment) {
        this.active = active;
        this.type = type;
        this.nationKey = nationKey;
        this.name = name;
        this.regionKey = regionKey;
        this.comment = comment;
    }

    @Override
    public String toString() {
        return "Nation{" +
                "nationKey=" + nationKey +
                ", name='" + name + '\'' +
                ", regionKey=" + regionKey +
                ", comment='" + comment + '\'' +
                ", active=" + active +
                ", type='" + type + '\'' +
                '}';
    }

    public Nation(String[] fields) {
        this.active = fields[0].equals("+");;
        this.type = fields[1];
        this.nationKey = Integer.parseInt(fields[2]);
        this.name = fields[3];
        this.regionKey = Integer.parseInt(fields[4]);
        this.comment = fields[5];
    }

}
