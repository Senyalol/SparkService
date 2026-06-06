package spark;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AnomalyDetectionFunction implements FlatMapGroupsWithStateFunction<Integer, Row, String, AlertEvent> {

    private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionFunction.class);
    private static final String ANOMALY_STATE_SEP = "||";
    private static final long WINDOW_ANOMALY_MS = TimeUnit.MINUTES.toMillis(Long.parseLong(System.getenv("WINDOW_ANOMALY_MS")));
    private static final long WINDOW_RFM_MS = TimeUnit.MINUTES.toMillis(Long.parseLong(System.getenv("WINDOW_RFM_MS")));
    private static final int MIN_REVERSAL_COUNT = Integer.parseInt(System.getenv("MIN_REVERSAL_COUNT"));                                   // Минимум 2 таких паттерна
    private static final long REVERSAL_WINDOW_MS = TimeUnit.MINUTES.toMillis(Long.parseLong(System.getenv("REVERSAL_WINDOW_MS")));     // Окно 5 минут
    private static final double REVERSAL_THRESHOLD = Double.parseDouble(System.getenv("REVERSAL_THRESHOLD"));                              // Кредит >= 90% от депозита
    private static final long MAX_TIME_BETWEEN_MS = TimeUnit.MINUTES.toMillis(Long.parseLong(System.getenv("MAX_TIME_BETWEEN_MS")));     // Максимум 1 минута между операциями
    private static final double SMALL_TRANSACTION_THRESHOLD = Double.parseDouble(System.getenv("SMALL_TRANSACTION_THRESHOLD"));
    private static final int MIN_STRUCTURING_COUNT = Integer.parseInt(System.getenv("MIN_STRUCTURING_COUNT"));
    private static final double MIN_STRUCTURING_TOTAL = Double.parseDouble(System.getenv("MIN_STRUCTURING_TOTAL"));          // Минимальная общая сумма
    private static final long STRUCTURING_WINDOW_MS = TimeUnit.MINUTES.toMillis(Long.parseLong(System.getenv("STRUCTURING_WINDOW_MS")));


    @Override
    public Iterator<AlertEvent> call(Integer userId, Iterator<Row> rows, GroupState<String> state) throws Exception {
        List<AlertEvent> out = new ArrayList<>();

        if (state.hasTimedOut()) {
            log.debug("State timed out for user: {}", userId);
            state.remove();
            return out.iterator();
        }

        state.setTimeoutDuration(TimeUnit.MINUTES.toMillis(5));

        String stateStr = state.exists() ? state.get() : "";
        long now = System.currentTimeMillis();

        List<TransactionEntry> avgEntries = new ArrayList<>();
        List<TransactionEntry> mEntries = new ArrayList<>();
        loadAnomalyState(stateStr, now, avgEntries, mEntries);

        int transactionCount = 0;
        while (rows.hasNext()) {
            Row r = rows.next();
            transactionCount++;
            long eventTime = r.getLong(3);
            String type = r.getString(1);
            double sum = r.getDouble(2);
            long curr = eventTime > 0 ? eventTime : now;

            purgeWindow(mEntries, curr, WINDOW_RFM_MS);
            double currentM = segmentBalanceFromEntries(mEntries);

            if (isNegativeMCredit(currentM, type, sum)) {
                AlertEvent alert = new AlertEvent();
                alert.setUser_id(userId);
                alert.setEvent_time(eventTime);
                alert.setType(type);
                alert.setSum(sum);
                alert.setAvg_check_5min(currentM);
                alert.setMessage(AnomalyType.NEGATIVE_M.name());
                out.add(alert);
                continue;
            }

            double avg = calculateAverage(avgEntries);

            if (avg > 0 && sum >= 3 * avg) {
                AlertEvent alert = new AlertEvent();
                alert.setUser_id(userId);
                alert.setEvent_time(eventTime);
                alert.setType(type);
                alert.setSum(sum);
                alert.setAvg_check_5min(avg);
                alert.setMessage(AnomalyType.BIGGER_THEN_AVG_CHECK.name());
                out.add(alert);
            }

            if (isStructuringSmallTransactions(mEntries, sum, curr)) {
                AlertEvent alert = new AlertEvent();
                alert.setUser_id(userId);
                alert.setEvent_time(eventTime);
                alert.setType(type);
                alert.setSum(sum);
                alert.setAvg_check_5min(0);
                alert.setMessage(AnomalyType.STRUCTURING_SMALL_TRANSACTIONS.name());
                out.add(alert);
            }

            if (isExcessiveReversalPattern(mEntries, type, sum, curr)) {
                AlertEvent alert = new AlertEvent();
                alert.setUser_id(userId);
                alert.setEvent_time(eventTime);
                alert.setType(type);
                alert.setSum(sum);
                alert.setAvg_check_5min(0);
                alert.setMessage(AnomalyType.EXCESSIVE_REVERSAL_PATTERN.name());
                out.add(alert);
            }

            long entryTs = eventTime > 0 ? eventTime : now;
            mEntries.add(new TransactionEntry(entryTs, sum, type));
            avgEntries.add(new TransactionEntry(entryTs, sum, type));
        }

        if (transactionCount > 0 && log.isDebugEnabled()) {
            log.debug("Processed {} transactions for user {}, avg window={}, m window={}",
                    transactionCount, userId, avgEntries.size(), mEntries.size());
        }

        state.update(serializeAnomalyState(avgEntries, mEntries));

        return out.iterator();
    }

    private List<TransactionEntry> parseStateEntries(String stateStr, long now, long windowMs) {
        List<TransactionEntry> entries = new ArrayList<>();
        if (stateStr == null || stateStr.isEmpty()) {
            return entries;
        }

        for (String e : stateStr.split(",")) {
            if (e.isEmpty()) continue;
            String[] parts = e.split(":");
            if (parts.length >= 2) {
                try {
                    long ts = Long.parseLong(parts[0]);
                    if (now - ts <= windowMs) {
                        double sum = Double.parseDouble(parts[1]);
                        String type = parts.length >= 3 ? parts[2] : "";
                        entries.add(new TransactionEntry(ts, sum, type));
                    }
                } catch (NumberFormatException ex) {
                    log.error("Failed to parse state entry: {}", e, ex);
                }
            }
        }
        return entries;
    }

    private double calculateAverage(List<TransactionEntry> entries) {
        if (entries.isEmpty()) return 0;
        double total = 0;
        for (TransactionEntry e : entries) {
            total += e.getSum();
        }
        return total / entries.size();
    }

    private void loadAnomalyState(String stateStr, long now,
                                  List<TransactionEntry> avgEntries, List<TransactionEntry> mEntries) {
        if (stateStr == null || stateStr.isEmpty()) {
            return;
        }
        int sep = stateStr.indexOf(ANOMALY_STATE_SEP);
        if (sep >= 0) {
            avgEntries.addAll(parseStateEntries(stateStr.substring(0, sep), now, WINDOW_ANOMALY_MS));
            mEntries.addAll(parseEntriesCsv(stateStr.substring(sep + ANOMALY_STATE_SEP.length())));
        } else {
            avgEntries.addAll(parseStateEntries(stateStr, now, WINDOW_ANOMALY_MS));
        }
    }

    private List<TransactionEntry> parseEntriesCsv(String csv) {
        List<TransactionEntry> entries = new ArrayList<>();
        if (csv == null || csv.isEmpty()) {
            return entries;
        }
        for (String e : csv.split(",")) {
            if (e.isEmpty()) {
                continue;
            }
            String[] parts = e.split(":");
            if (parts.length >= 3) {
                try {
                    long ts = Long.parseLong(parts[0]);
                    double sum = Double.parseDouble(parts[1]);
                    String type = parts[2];
                    entries.add(new TransactionEntry(ts, sum, type));
                } catch (NumberFormatException ex) {
                    log.error("Failed to parse M state entry: {}", e, ex);
                }
            }
        }
        return entries;
    }

    private String serializeEntries(List<TransactionEntry> entries) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < entries.size(); i++) {
            if (i > 0) sb.append(",");
            TransactionEntry e = entries.get(i);
            sb.append(e.getTimestamp()).append(":").append(e.getSum()).append(":").append(e.getType());
        }
        return sb.toString();
    }

    private String serializeAnomalyState(List<TransactionEntry> avgEntries, List<TransactionEntry> mEntries) {
        return serializeEntries(avgEntries) + ANOMALY_STATE_SEP + serializeEntries(mEntries);
    }

    public static void purgeWindow(List<TransactionEntry> entries, long curr, long windowMs) {
        entries.removeIf(entry -> curr - entry.getTimestamp() > windowMs);
    }

    public static boolean isNegativeMCredit(double currentM, String type, double sum) {
        return "Credit".equalsIgnoreCase(type) && sum > currentM;
    }

    /** Баланс в окне RFM: Deposit +, Credit −; M не ниже 0. */
    public static double segmentBalanceFromEntries(List<TransactionEntry> entries) {
        double balance = 0;
        for (TransactionEntry e : entries) {
            String t = e.getType();
            if (t == null) {
                continue;
            }
            if ("Deposit".equalsIgnoreCase(t)) {
                balance += e.getSum();
            } else if ("Credit".equalsIgnoreCase(t)) {
                balance -= e.getSum();
            }
        }
        return clampM(balance);
    }

    public static double clampM(double m) {
        return Math.max(0, m);
    }


    private static boolean isExcessiveReversalPattern(
            List<TransactionEntry> entries,
            String currentType,
            double currentSum,
            long currentTime
    ) {
        // Проверяем только Credit транзакции (вывод средств)
        if (!"Credit".equalsIgnoreCase(currentType)) {
            return false;
        }

        int reversalCount = 0;

        // Проходим по всем транзакциям в окне
        for (TransactionEntry e : entries) {
            // Ищем Deposit (внесение средств)
            if ("Deposit".equalsIgnoreCase(e.getType())) {
                // Проверяем, была ли транзакция в пределах окна
                if (currentTime - e.getTimestamp() <= REVERSAL_WINDOW_MS) {
                    // Проверяем, что кредит составляет >= REVERSAL_THRESHOLD от депозита
                    boolean amountMatch = currentSum >= e.getSum() * REVERSAL_THRESHOLD;
                    // Проверяем, что время между операциями не превышает MAX_TIME_BETWEEN_MS
                    boolean timeMatch = (currentTime - e.getTimestamp()) <= MAX_TIME_BETWEEN_MS;

                    if (amountMatch && timeMatch) {
                        reversalCount++;
                    }
                }
            }
        }

        return reversalCount >= MIN_REVERSAL_COUNT;
    }

    private static boolean isStructuringSmallTransactions(
            List<TransactionEntry> entries,
            double currentSum,
            long currentTime
    ) {
        long smallCount = 0;
        double smallTotal = 0;

        for (TransactionEntry e : entries) {
            if (currentTime - e.getTimestamp() <= STRUCTURING_WINDOW_MS) {
                if (e.getSum() <= SMALL_TRANSACTION_THRESHOLD) {
                    smallCount++;
                    smallTotal += e.getSum();
                }
            }
        }

        if (currentSum <= SMALL_TRANSACTION_THRESHOLD) {
            smallCount++;
            smallTotal += currentSum;
        }

        return smallCount >= MIN_STRUCTURING_COUNT &&
                smallTotal >= MIN_STRUCTURING_TOTAL;
    }



}