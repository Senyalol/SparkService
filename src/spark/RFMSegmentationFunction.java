package spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

class RFMSegmentationFunction implements FlatMapGroupsWithStateFunction<Integer, Row, String, SegmentEvent> {

    private static final Logger log = LoggerFactory.getLogger(RFMSegmentationFunction.class);
    private static final double SLEEPING_R_MINUTES = Double.parseDouble(System.getenv("SLEEPING_R_MINUTES"));
    private static final double NEWCOMER_HOURS = Double.parseDouble(System.getenv("NEWCOMER_HOURS"));
    private static final long WINDOW_RFM_MS = TimeUnit.MINUTES.toMillis(Long.parseLong(System.getenv("WINDOW_RFM_MS")));

    private transient redis.clients.jedis.Jedis jedis;
    private transient ObjectMapper mapper;

    private final ObjectMapper createMapper() {
        ObjectMapper m = new ObjectMapper();
        m.registerModule(new JavaTimeModule());
        m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return m;
    }

    @Override
    public Iterator<SegmentEvent> call(Integer userId, Iterator<Row> rows, GroupState<String> state) throws Exception {
        List<SegmentEvent> out = new ArrayList<>();

        if (state.hasTimedOut()) {
            log.debug("State timed out for user: {}", userId);
            state.remove();
            return out.iterator();
        }

        state.setTimeoutDuration(TimeUnit.HOURS.toMillis(24));

        // Инициализация Redis клиента (lazy, внутри executor'а)
        if (jedis == null) {
            String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
            int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
            jedis = new redis.clients.jedis.Jedis(redisHost, redisPort);
            log.info("Connected to Redis at {}:{}", redisHost, redisPort);
        }

        // Инициализация ObjectMapper (lazy)
        if (mapper == null) {
            mapper = createMapper();
        }

        String redisKey = "rfm:" + userId;
        String savedStateJson = jedis.get(redisKey);

        RFMState rfmState = resolveInitialRfmState(userId, savedStateJson, state);

        while (rows.hasNext()) {
            Row r = rows.next();
            long eventTime = r.getLong(3);
            String type = r.getString(1);
            double sum = r.getDouble(2);

            RfmUpdateResult update = updateRFMState(rfmState, eventTime, sum, type);
            if (!update.applied) {
                log.debug("Rejected Credit for user {}: sum={}, current M={}", userId, sum, rfmState.getMWindow());
                continue;
            }
            rfmState = update.state;
            String segment = calculateSegment(rfmState, eventTime, userId);

            SegmentEvent ev = new SegmentEvent();
            ev.setUser_id(userId);
            ev.setSegment(segment);
            ev.setR_minutes(rfmState.getRMinutes());
            ev.setF(rfmState.getFWindow());
            ev.setM(rfmState.getMWindow());
            ev.setUpdated_at(eventTime);
            out.add(ev);
        }

        if (!out.isEmpty()) {
            String newStateJson = mapper.writeValueAsString(rfmState);
            jedis.set(redisKey, newStateJson);
            jedis.expire(redisKey, 604800);
            log.debug("Saved user {} to Redis: f={}, m={}", userId, rfmState.getFWindow(), rfmState.getMWindow());
        }

        state.update(serializeRFMState(rfmState));

        return out.iterator();
    }

    /**
     * Состояние из Redis и из Spark checkpoint; на новом executor lastTs не теряется.
     */
    private RFMState resolveInitialRfmState(Integer userId, String savedStateJson, GroupState<String> state)
            throws Exception {
        RFMState fromRedis = null;
        if (savedStateJson != null && !savedStateJson.isEmpty()) {
            fromRedis = mapper.readValue(savedStateJson, RFMState.class);
            ensureEntriesNotNull(fromRedis);
            reconcileLastTsFromEntries(fromRedis);

            // Если в Redis нет mTotal, инициализируем из оконного M
            if (fromRedis.getMTotal() == 0 && fromRedis.getMWindow() > 0) {
                fromRedis.setMTotal(fromRedis.getMWindow());
            }

            // Если в Redis нет fTotal, инициализируем из оконного F
            if (fromRedis.getFTotal() == 0 && fromRedis.getFWindow() > 0) {
                fromRedis.setFTotal(fromRedis.getFWindow());
            }

            log.debug("Loaded user {} from Redis: lastTs={}, fWindow={}, fTotal={}, mTotal={}, mWindow={}",
                    userId, fromRedis.getLastTs(), fromRedis.getFWindow(),
                    fromRedis.getFTotal(), fromRedis.getMTotal(), fromRedis.getMWindow());
        }

        RFMState fromSpark = null;
        if (state.exists()) {
            fromSpark = parseRFMState(state.get());
            ensureEntriesNotNull(fromSpark);
            reconcileLastTsFromEntries(fromSpark);
            log.debug("Loaded user {} from Spark state: lastTs={}", userId, fromSpark.getLastTs());
        }

        RFMState rfmState;
        if (fromRedis != null && fromSpark != null) {
            rfmState = fromRedis;
            rfmState.setLastTs(Math.max(fromRedis.getLastTs(), fromSpark.getLastTs()));
            rfmState.setLastWallMs(Math.max(fromRedis.getLastWallMs(), fromSpark.getLastWallMs()));
            rfmState.setFirstTs(mergeFirstTs(fromRedis.getFirstTs(), fromSpark.getFirstTs()));


            if (fromSpark.getMTotal() > fromRedis.getMTotal()) {
                rfmState.setMTotal(fromSpark.getMTotal());
            }


            if (fromSpark.getFTotal() > fromRedis.getFTotal()) {
                rfmState.setFTotal(fromSpark.getFTotal());
            }

            if (fromSpark.getEntries().size() > fromRedis.getEntries().size()) {
                rfmState.setEntries(new ArrayList<>(fromSpark.getEntries()));
            }
            recomputeAggregates(rfmState);
        } else if (fromRedis != null) {
            rfmState = fromRedis;
        } else if (fromSpark != null) {
            rfmState = fromSpark;
        } else {
            rfmState = new RFMState();
            log.debug("New user: {}", userId);
        }

        reconcileLastTsFromEntries(rfmState);
        return rfmState;
    }

    private static void ensureEntriesNotNull(RFMState s) {
        if (s.getEntries() == null) {
            s.setEntries(new ArrayList<>());
        }
    }

    /** Восстановить lastTs из истории, если после JSON он не прочитался (0). */
    private static void reconcileLastTsFromEntries(RFMState s) {
        if (s.getLastTs() != 0 || s.getEntries() == null || s.getEntries().isEmpty()) {
            return;
        }
        long maxTs = 0;
        for (TransactionEntry e : s.getEntries()) {
            if (e.getTimestamp() > maxTs) {
                maxTs = e.getTimestamp();
            }
        }
        if (maxTs > 0) {
            s.setLastTs(maxTs);
        }
    }

    private static long mergeFirstTs(long a, long b) {
        if (a == 0) {
            return b;
        }
        if (b == 0) {
            return a;
        }
        return Math.min(a, b);
    }

    private static void recomputeAggregates(RFMState s) {
        s.setFWindow(s.getEntries().size());
        s.setMWindow(segmentBalanceFromEntries(s.getEntries()));
        // F_total и M_total НЕ пересчитываем из entries! Они хранятся отдельно
    }

    private RFMState parseRFMState(String stateStr) {
        RFMState state = new RFMState();
        state.setLastTs(0);
        state.setFirstTs(0);
        state.setLastWallMs(0);
        state.setMTotal(0);
        state.setMWindow(0);
        state.setEntries(new ArrayList<>());

        if (stateStr == null || stateStr.isEmpty()) {
            return state;
        }

        String[] parts = stateStr.split("\\|", 7);
        if (parts.length < 6) {
            return state;
        }

        try {
            state.setLastTs(Long.parseLong(parts[0]));
            state.setFirstTs(Long.parseLong(parts[1]));
            state.setLastWallMs(Long.parseLong(parts[2]));
            state.setMTotal(Double.parseDouble(parts[3]));
            state.setMWindow(Double.parseDouble(parts[4]));
            state.setFTotal(Long.parseLong(parts[5]));
            if (parts.length >= 7) {
                parseRFMEntries(parts[6], state);
            }
            state.setFWindow(state.getEntries().size());  // F_window из размера entries
        } catch (NumberFormatException ex) {
            log.error("Failed to parse RFM state: {}", stateStr, ex);
        }

        return state;
    }


    private void parseRFMEntries(String csv, RFMState state) {
        if (csv == null || csv.isEmpty()) {
            return;
        }
        for (String e : csv.split(",")) {
            if (e.isEmpty()) {
                continue;
            }
            String[] p = e.split(":");
            if (p.length >= 3) {
                try {
                    long ts = Long.parseLong(p[0]);
                    double sum = Double.parseDouble(p[1]);
                    String type = p[2];
                    state.getEntries().add(new TransactionEntry(ts, sum, type));
                } catch (NumberFormatException ex) {
                    log.error("Failed to parse RFM entry: {}", e, ex);
                }
            }
        }
    }

    private RfmUpdateResult updateRFMState(RFMState state, long eventTime, double sum, String type) {
        long wallNow = System.currentTimeMillis();

        // ========== НОРМАЛИЗАЦИЯ ВРЕМЕНИ ==========
        long curr;
        if (eventTime > 0) {
            if (eventTime < 10_000_000_000L) {
                curr = eventTime * 1000;
                log.debug("Converted eventTime from seconds to ms: {} -> {}", eventTime, curr);
            } else {
                curr = eventTime;
            }
        } else {
            curr = wallNow;
            log.debug("Using wallNow as ms: {}", curr);
        }

        // ========== ДИАГНОСТИКА ==========
        System.out.println("=== DIAGNOSTIC RMINUTES ===");
        System.out.println("eventTime raw: " + eventTime);
        System.out.println("curr: " + curr);
        System.out.println("state.lastTs: " + state.getLastTs());
        System.out.println("state.rMinutes before: " + state.getRMinutes());
        System.out.println("===========================");

        // ========== ОСНОВНАЯ ЛОГИКА ==========

        // 1. Обновляем оконные метрики (F_window и M_window)
        purgeWindow(state.getEntries(), curr, WINDOW_RFM_MS);
        double currentMWindow = segmentBalanceFromEntries(state.getEntries());
        state.setMWindow(currentMWindow);
        state.setFWindow(state.getEntries().size());

        // 2. Проверка на отрицательный M
        if (isNegativeMCredit(currentMWindow, type, sum)) {
            return new RfmUpdateResult(state, false);
        }

        // 3. Обновляем M_TOTAL и F_TOTAL
        double currentMTotal = state.getMTotal();
        long currentFTotal = state.getFTotal();

        if ("Deposit".equalsIgnoreCase(type)) {
            currentMTotal += sum;
        } else if ("Credit".equalsIgnoreCase(type)) {
            currentMTotal -= sum;
        }
        currentFTotal++;

        state.setMTotal(Math.max(0, currentMTotal));
        state.setFTotal(currentFTotal);

        // 4. Устанавливаем firstTs (первая транзакция)
        if (state.getFirstTs() == 0) {
            state.setFirstTs(curr);
        }

        // РАСЧЁТ R_MINUTES
        double newRMinutes = state.getRMinutes();

        if (state.getLastTs() == 0) {
            newRMinutes = 0;
            log.debug("First transaction, rMinutes = 0");
        } else {
            long deltaEvent = curr - state.getLastTs();

            if (deltaEvent > 0) {
                newRMinutes = deltaEvent / 60000.0;
                log.info("RMinutes: deltaEvent={}ms ({} min), rMinutes={}",
                        deltaEvent, deltaEvent / 60000.0, newRMinutes);
            } else if (deltaEvent == 0) {
                log.debug("Same timestamp, keeping previous rMinutes={}", newRMinutes);
            } else {
                log.warn("Time went backwards! curr={}, lastTs={}", curr, state.getLastTs());
                newRMinutes = 0;
            }
        }

        state.setRMinutes(newRMinutes);

        // 5. Обновляем lastTs
        if (curr > state.getLastTs() || state.getLastTs() == 0) {
            state.setLastTs(curr);
        }
        state.setLastWallMs(wallNow);

        // 6. Добавляем транзакцию в окно
        state.getEntries().add(new TransactionEntry(curr, sum, type));
        purgeWindow(state.getEntries(), curr, WINDOW_RFM_MS);

        // 7. Пересчитываем оконные метрики
        state.setFWindow(state.getEntries().size());
        state.setMWindow(segmentBalanceFromEntries(state.getEntries()));

        return new RfmUpdateResult(state, true);
    }

    private String calculateSegment(RFMState state, long currentTime, int userId) {
        double firstHoursAgo = (currentTime - state.getFirstTs()) / 3600000.0;

        System.out.println("DEBUG: user=" + userId +
                ", firstTs=" + state.getFirstTs() +
                ", currentTime=" + currentTime +
                ", firstHoursAgo=" + firstHoursAgo);

        if (firstHoursAgo < NEWCOMER_HOURS) {
            return "Новичок";
        }
        else if (state.getMTotal() > 500000 && state.getFTotal() > 30) {
            return "VIP";
        }
        else if (state.getMTotal() > 20000 && state.getFTotal() > 10) {
            return "Активный";
        }
        else if (state.getRMinutes() > SLEEPING_R_MINUTES) {
            return "Спящий";
        }
        else {
            return "Стандартный";
        }
    }



    private String serializeRFMState(RFMState state) {
        StringBuilder sb = new StringBuilder();
        sb.append(state.getLastTs()).append("|")
                .append(state.getFirstTs()).append("|")
                .append(state.getLastWallMs()).append("|")
                .append(state.getMTotal()).append("|")  // ← НОВОЕ поле
                .append(state.getMWindow()).append("|") // ← НОВОЕ поле
                .append(state.getFTotal()).append("|");

        for (int i = 0; i < state.getEntries().size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            TransactionEntry e = state.getEntries().get(i);
            sb.append(e.getTimestamp()).append(":").append(e.getSum()).append(":").append(e.getType());
        }
        return sb.toString();
    }

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

    public static void purgeWindow(List<TransactionEntry> entries, long curr, long windowMs) {
        entries.removeIf(entry -> curr - entry.getTimestamp() > windowMs);
    }

    public static boolean isNegativeMCredit(double currentM, String type, double sum) {
        return "Credit".equalsIgnoreCase(type) && sum > currentM;
    }


}