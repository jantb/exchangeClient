package client;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class Tick {
    private LocalDateTime dateTime;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal avg;
    private BigDecimal vol_curr_day;
    private BigDecimal last;
    private BigDecimal ask;
    private BigDecimal bid;

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public BigDecimal getHigh() {
        return high;
    }

    public BigDecimal getLow() {
        return low;
    }

    public BigDecimal getAvg() {
        return avg;
    }

    public BigDecimal getVol_curr_day() {
        return vol_curr_day;
    }

    public BigDecimal getLast() {
        return last;
    }

    public BigDecimal getAsk() {
        return ask;
    }

    public BigDecimal getBid() {
        return bid;
    }

    @Override
    public String toString() {
        return "Tick{" +
                "dateTime='" + dateTime + '\'' +
                ", high='" + high + '\'' +
                ", low='" + low + '\'' +
                ", avg='" + avg + '\'' +
                ", vol_curr_day='" + vol_curr_day + '\'' +
                ", last='" + last + '\'' +
                ", ask='" + ask + '\'' +
                ", bid='" + bid + '\'' +
                '}';
    }
}
