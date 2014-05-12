package client;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class DynamicTimeseries {
    private final NavigableMap<LocalDateTime, BigDecimal> map = new TreeMap<>();

    public void put(LocalDateTime dateTime, BigDecimal bigDecimal) {
        map.put(dateTime, bigDecimal);
    }

    public BigDecimal get(LocalDateTime datetime) {
        Map.Entry<LocalDateTime, BigDecimal> dateTimeBigDecimalEntry = map.floorEntry(datetime);
        if (dateTimeBigDecimalEntry == null) {
            return null;
        }
        return dateTimeBigDecimalEntry.getValue();
    }

    public BigDecimal getWMVA(TemporalAmount duration, TemporalAmount sample, LocalDateTime now) {
        LocalDateTime then = now.minus(duration);
        NavigableMap<LocalDateTime, BigDecimal> tailMap = map.tailMap(then, true);
        if (map.isEmpty()) {
            return BigDecimal.ZERO;
        }
        BigDecimal thenVal = get(then);
        if (thenVal == null) {
            return map.ceilingEntry(then).getValue();
        }
        tailMap.put(then, thenVal);
        Collection<BigDecimal> values = new ArrayList<>();
        while (now.isAfter(then)) {
            Map.Entry<LocalDateTime, BigDecimal> entry = tailMap.floorEntry(now);
            if (entry == null) {
                throw new IllegalArgumentException("No valuefor " + now);
            }
            BigDecimal value = entry.getValue();
            now = now.minus(sample);
            values.add(value);
        }
        BigDecimal movingAverage = BigDecimal.ZERO;
        long valueSize = values.size();
        long sum = 0;
        for (BigDecimal value : values) {
            sum += valueSize;
            movingAverage = movingAverage.add(value.multiply(new BigDecimal(valueSize--)));
        }
        return movingAverage.divide(BigDecimal.valueOf(sum), 5, RoundingMode.HALF_UP);
    }


    public BigDecimal getMVA(TemporalAmount duration, TemporalAmount sample, LocalDateTime now) {
        LocalDateTime then = now.minus(duration);
        NavigableMap<LocalDateTime, BigDecimal> tailMap = map.tailMap(then, true);
        if (map.isEmpty()) {
            return BigDecimal.ZERO;
        }
        BigDecimal thenVal = get(then);
        if (thenVal == null) {
            return map.ceilingEntry(then).getValue();
        }
        tailMap.put(then, thenVal);
        Collection<BigDecimal> values = new ArrayList<>();
        while (now.isAfter(then)) {
            Map.Entry<LocalDateTime, BigDecimal> entry = tailMap.floorEntry(now);
            if (entry == null) {
                throw new IllegalArgumentException("No valuefor " + now);
            }
            BigDecimal value = entry.getValue();
            now = now.minus(sample);
            values.add(value);
        }
        BigDecimal movingAverage = BigDecimal.ZERO;
        for (BigDecimal value : values) {
            movingAverage = movingAverage.add(value);
        }
        return movingAverage.divide(BigDecimal.valueOf(values.size()), 5, RoundingMode.HALF_UP);
    }
}
