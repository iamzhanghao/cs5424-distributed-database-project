package clients.utils;

import java.util.Objects;

public class Customer {


    public int w_id;
    public int d_id;
    public int c_id;

    public Customer(int w_id, int d_id, int c_id) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Customer)) return false;
        Customer customer = (Customer) o;
        return w_id == customer.w_id && d_id == customer.d_id && c_id == customer.c_id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(w_id, d_id, c_id);
    }

}
