package clients.utils;

import java.util.Comparator;

public class CustomerBalanceComparator implements Comparator<CustomerBalance> {
    public int compare(CustomerBalance c1, CustomerBalance c2) {
        if(c1.balance.compareTo(c2.balance) == -1) {
            return 1;
        }
        else if(c1.balance.compareTo(c2.balance) == 1) {
            return -1;
        }
        else {
            return 0;
        }
    }
}
