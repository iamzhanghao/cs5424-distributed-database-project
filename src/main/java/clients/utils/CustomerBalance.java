package clients.utils;

import java.math.BigDecimal;

public class CustomerBalance {

    public int wid;
    public int did;
    public String name_first;
    public String name_middle;
    public String name_last;
    public BigDecimal balance;

    public CustomerBalance(int wid, int did, String name_first, String name_middle, String name_last, BigDecimal balance) {
        this.wid = wid;
        this.did = did;
        this.name_first = name_first;
        this.name_middle = name_middle;
        this.name_last = name_last;
        this.balance = balance;
    }

    public void printInfo() {
        System.out.println(this.name_first + ' ' + this.name_middle + ' ' + this.name_last);
        System.out.println(this.balance);
    }
}
