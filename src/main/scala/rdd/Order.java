package rdd;

import java.util.Date;

/**
 * @author hongtao.hao
 * @date 2020/6/5
 */
public class Order {
    private Integer id;
    private Integer phone;
    private Date date;

    public Order(Integer id, Integer phone, Date date) {
        this.id = id;
        this.phone = phone;
        this.date = date;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getPhone() {
        return phone;
    }

    public void setPhone(Integer phone) {
        this.phone = phone;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
