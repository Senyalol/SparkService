package Data;

import java.math.BigDecimal;

public class Data {
    private Integer user_id;
    private String firstname;
    private String lastname;
    private TransactionType type;
    private BigDecimal sum;

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public String getFirstname() {
        return firstname;
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public TransactionType getType() {
        return type;
    }

    public void setType(TransactionType type) {
        this.type = type;
    }

    public BigDecimal getSum() {
        return sum;
    }

    public void setSum(BigDecimal sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "Data{user_id=" + user_id + ", firstname='" + firstname + "', lastname='" + lastname +
                "', type=" + type + ", sum=" + sum + "}";
    }

}