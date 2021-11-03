package clients.utils;

import java.util.Objects;

public class Order {
    public int w_id;
    public int d_id;
    public int o_id;

    public Order(int w_id, int d_id, int o_id) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.o_id = o_id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Order)) return false;
        Order order = (Order) o;
        return w_id == order.w_id && d_id == order.d_id && o_id == order.o_id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(w_id, d_id, o_id);
    }

    @Override
    public String toString() {
        return String.format("%d,%d,%d",w_id,d_id,o_id);
    }
}
