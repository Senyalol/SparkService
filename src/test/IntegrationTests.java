package test;

import org.junit.jupiter.api.*;
import spark.RFMState;
import spark.TransactionEntry;

import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static spark.AnomalyDetectionFunction.purgeWindow;
import static spark.AnomalyDetectionFunction.segmentBalanceFromEntries;

@DisplayName("Integration Tests")
class IntegrationTests {

    @Nested
    @DisplayName("State Serialization/Deserialization Tests")
    class StateSerializationTests {

        @Test
        @DisplayName("Should serialize and deserialize RFMState correctly")
        void shouldSerializeDeserializeRFMState() {
            // Create original state
            RFMState original = new RFMState();
            original.setLastTs(1234567890L);
            original.setFirstTs(1234567000L);
            original.setLastWallMs(1234567890L);
            original.setMTotal(5000.50);
            original.setMWindow(1500.25);
            original.setFTotal(42L);
            original.setFWindow(10L);
            original.setRMinutes(15.5);

            original.getEntries().add(new TransactionEntry(1000L, 100.0, "Deposit"));
            original.getEntries().add(new TransactionEntry(2000L, 50.0, "Credit"));

            // Serialize to string format
            String serialized = serializeRFMState(original);
            assertNotNull(serialized);
            System.out.println("Serialized: " + serialized);

            // Deserialize
            RFMState deserialized = deserializeRFMState(serialized);

            // Verify all fields
            assertEquals(original.getLastTs(), deserialized.getLastTs());
            assertEquals(original.getFirstTs(), deserialized.getFirstTs());
            assertEquals(original.getLastWallMs(), deserialized.getLastWallMs());
            assertEquals(original.getMTotal(), deserialized.getMTotal(), 0.001);
            assertEquals(original.getMWindow(), deserialized.getMWindow(), 0.001);
            assertEquals(original.getFTotal(), deserialized.getFTotal());
            assertEquals(original.getFWindow(), deserialized.getFWindow());
            assertEquals(original.getRMinutes(), deserialized.getRMinutes(), 0.001);
            assertEquals(original.getEntries().size(), deserialized.getEntries().size());
        }

        @Test
        @DisplayName("Should handle empty state serialization")
        void shouldHandleEmptyStateSerialization() {
            RFMState empty = new RFMState();

            String serialized = serializeRFMState(empty);
            RFMState deserialized = deserializeRFMState(serialized);

            assertEquals(0, deserialized.getEntries().size());
            assertEquals(0, deserialized.getMTotal(), 0.001);
            assertEquals(0, deserialized.getFTotal());
            assertEquals(0, deserialized.getRMinutes(), 0.001);
        }

        private String serializeRFMState(RFMState state) {
            StringBuilder sb = new StringBuilder();
            // Добавляем ВСЕ поля, включая rMinutes!
            sb.append(state.getLastTs()).append("|")
                    .append(state.getFirstTs()).append("|")
                    .append(state.getLastWallMs()).append("|")
                    .append(state.getMTotal()).append("|")
                    .append(state.getMWindow()).append("|")
                    .append(state.getFTotal()).append("|")
                    .append(state.getFWindow()).append("|")      // ← ДОБАВИТЬ FWindow
                    .append(state.getRMinutes()).append("|");    // ← ДОБАВИТЬ rMinutes

            for (int i = 0; i < state.getEntries().size(); i++) {
                if (i > 0) sb.append(",");
                TransactionEntry e = state.getEntries().get(i);
                sb.append(e.getTimestamp()).append(":").append(e.getSum()).append(":").append(e.getType());
            }
            return sb.toString();
        }

        private RFMState deserializeRFMState(String stateStr) {
            RFMState state = new RFMState();
            if (stateStr == null || stateStr.isEmpty()) return state;

            // Увеличиваем до 9 частей (было 7)
            String[] parts = stateStr.split("\\|", 9);
            if (parts.length >= 8) {  // Минимум 8 полей (lastTs, firstTs, lastWallMs, mTotal, mWindow, fTotal, fWindow, rMinutes)
                state.setLastTs(Long.parseLong(parts[0]));
                state.setFirstTs(Long.parseLong(parts[1]));
                state.setLastWallMs(Long.parseLong(parts[2]));
                state.setMTotal(Double.parseDouble(parts[3]));
                state.setMWindow(Double.parseDouble(parts[4]));
                state.setFTotal(Long.parseLong(parts[5]));
                state.setFWindow(Long.parseLong(parts[6]));     // ← ДОБАВИТЬ
                state.setRMinutes(Double.parseDouble(parts[7])); // ← ДОБАВИТЬ

                // Entries are in part 8 if present
                if (parts.length >= 9 && !parts[8].isEmpty()) {
                    for (String e : parts[8].split(",")) {
                        if (e.isEmpty()) continue;
                        String[] p = e.split(":");
                        if (p.length >= 3) {
                            try {
                                state.getEntries().add(new TransactionEntry(
                                        Long.parseLong(p[0]),
                                        Double.parseDouble(p[1]),
                                        p[2]
                                ));
                            } catch (NumberFormatException ex) {
                                System.err.println("Failed to parse entry: " + e);
                            }
                        }
                    }
                }
            }
            return state;
        }
    }

    @Nested
    @DisplayName("End-to-End Transaction Processing Tests")
    class EndToEndProcessingTests {

        @Test
        @DisplayName("Should process multiple users independently")
        void shouldProcessMultipleUsersIndependently() {
            Map<Integer, RFMState> userStates = new HashMap<>();

            // User 1: VIP trajectory
            RFMState user1 = new RFMState();
            processTransaction(user1, System.currentTimeMillis(), 10000.0, "Deposit");
            processTransaction(user1, System.currentTimeMillis() + 1000, 5000.0, "Deposit");
            processTransaction(user1, System.currentTimeMillis() + 2000, 100.0, "Credit");
            userStates.put(1, user1);

            // User 2: Standard user
            RFMState user2 = new RFMState();
            processTransaction(user2, System.currentTimeMillis(), 100.0, "Deposit");
            processTransaction(user2, System.currentTimeMillis() + 1000, 50.0, "Credit");
            userStates.put(2, user2);

            // User 1 should have higher totals
            assertTrue(user1.getMTotal() > user2.getMTotal());
            assertTrue(user1.getFTotal() > user2.getFTotal());

            // Verify no interference between users
            assertEquals(14900.0, user1.getMTotal(), 0.001);
            assertEquals(50.0, user2.getMTotal(), 0.001);
        }

        @Test
        @DisplayName("Should handle time window correctly across multiple transactions")
        void shouldHandleTimeWindowCorrectly() {
            RFMState state = new RFMState();
            long now = System.currentTimeMillis();

            // Add transactions over time
            processTransaction(state, now - 600000, 1000.0, "Deposit"); // 10 min ago (outside window)
            processTransaction(state, now - 180000, 500.0, "Deposit");  // 3 min ago (inside window)
            processTransaction(state, now - 60000, 200.0, "Credit");     // 1 min ago (inside window)

            // Purge old transactions
            purgeWindow(state.getEntries(), now, 300000);

            assertEquals(2, state.getEntries().size()); // Should keep only recent 2
            double mWindow = segmentBalanceFromEntries(state.getEntries());
            assertEquals(300.0, mWindow, 0.001); // 500 - 200 = 300
        }

        @Test
        @DisplayName("Should correctly calculate RMinutes from event times")
        void shouldCalculateRMinutesCorrectly() {
            RFMState state = new RFMState();
            long now = System.currentTimeMillis();

            // First transaction
            state.setLastTs(now - 120000); // 2 minutes ago
            state.setRMinutes(calculateRMinutes(state, now, now - 120000));

            assertEquals(2.0, state.getRMinutes(), 0.1);

            // Second transaction after 30 seconds
            state.setLastTs(now - 90000); // Update to 1.5 min ago
            state.setRMinutes(calculateRMinutes(state, now, now - 90000));

            assertEquals(1.5, state.getRMinutes(), 0.1);
        }

        private void processTransaction(RFMState state, long timestamp, double sum, String type) {
            if (state.getFirstTs() == 0) {
                state.setFirstTs(timestamp);
            }

            if ("Deposit".equalsIgnoreCase(type)) {
                state.setMTotal(state.getMTotal() + sum);
            } else if ("Credit".equalsIgnoreCase(type)) {
                state.setMTotal(Math.max(0, state.getMTotal() - sum));
            }
            state.setFTotal(state.getFTotal() + 1);
            state.setLastTs(timestamp);
            state.getEntries().add(new TransactionEntry(timestamp, sum, type));
        }

        private double calculateRMinutes(RFMState state, long curr, long lastTs) {
            if (lastTs == 0) return 0;
            long deltaEvent = curr - lastTs;
            if (deltaEvent > 0) {
                return deltaEvent / 60000.0;
            } else if (deltaEvent == 0) {
                return state.getRMinutes();
            } else {
                return 0;
            }
        }
    }

    @Nested
    @DisplayName("Edge Cases and Error Handling Tests")
    class EdgeCasesTests {

        @Test
        @DisplayName("Should handle very large time gaps")
        void shouldHandleLargeTimeGaps() {
            RFMState state = new RFMState();

            // First transaction
            processTransaction(state, 1000L, 1000.0, "Deposit");

            // Second transaction after 1 year (in seconds)
            long oneYearInSeconds = 31536000L;
            processTransaction(state, oneYearInSeconds, 500.0, "Deposit");

            // RMinutes should be huge but not cause overflow
            double rMinutes = calculateRMinutes(state, oneYearInSeconds * 1000, 1000L);
            assertTrue(rMinutes > 0);
            assertFalse(Double.isInfinite(rMinutes));
            assertFalse(Double.isNaN(rMinutes));
        }

        @Test
        @DisplayName("Should handle malformed transaction types gracefully")
        void shouldHandleMalformedTypes() {
            RFMState state = new RFMState();

            // Unknown transaction type should be ignored
            double beforeMTotal = state.getMTotal();

            processTransaction(state, System.currentTimeMillis(), 1000.0, "Unknown");

            // Should not affect totals
            assertEquals(beforeMTotal, state.getMTotal(), 0.001);
            // Note: F_total increments regardless of type in this implementation
        }

        @Test
        @DisplayName("Should handle null or missing fields")
        void shouldHandleNullFields() {
            TransactionEntry entry = new TransactionEntry();
            entry.setTimestamp(System.currentTimeMillis());
            entry.setSum(100.0);
            entry.setType(null);

            double balance = segmentBalanceFromEntries(List.of(entry));
            assertEquals(0.0, balance, 0.001, "Null type should be ignored");
        }

        @Test
        @DisplayName("Should handle concurrent-like sequence of transactions")
        void shouldHandleConcurrentTransactions() {
            RFMState state = new RFMState();
            long now = System.currentTimeMillis();

            // Multiple transactions with same timestamp (possible from batch)
            processTransaction(state, now, 100.0, "Deposit");
            processTransaction(state, now, 50.0, "Credit");
            processTransaction(state, now, 200.0, "Deposit");

            // All should be processed
            assertEquals(250.0, state.getMTotal(), 0.001);
            assertEquals(3, state.getFTotal());
        }

        private void processTransaction(RFMState state, long timestamp, double sum, String type) {
            if (state.getFirstTs() == 0 && timestamp > 0) {
                state.setFirstTs(timestamp);
            }

            if ("Deposit".equalsIgnoreCase(type)) {
                state.setMTotal(state.getMTotal() + sum);
            } else if ("Credit".equalsIgnoreCase(type)) {
                state.setMTotal(Math.max(0, state.getMTotal() - sum));
            }
            state.setFTotal(state.getFTotal() + 1);
            if (timestamp > state.getLastTs()) {
                state.setLastTs(timestamp);
            }
        }

        private double calculateRMinutes(RFMState state, long curr, long lastTs) {
            if (lastTs == 0) return 0;
            long deltaEvent = curr - lastTs;
            return deltaEvent > 0 ? deltaEvent / 60000.0 : 0;
        }
    }
}

@DisplayName("Performance and Load Tests")
class PerformanceTests {

    @Test
    @DisplayName("Should handle large number of entries in window")
    void shouldHandleLargeNumberOfEntries() {
        RFMState state = new RFMState();
        long now = System.currentTimeMillis();

        // Add 1000 transactions
        for (int i = 0; i < 1000; i++) {
            state.getEntries().add(new TransactionEntry(
                    now - (i * 1000),
                    Math.random() * 1000,
                    i % 2 == 0 ? "Deposit" : "Credit"
            ));
        }

        long startTime = System.nanoTime();
        purgeWindow(state.getEntries(), now, 300000);
        double balance = segmentBalanceFromEntries(state.getEntries());
        long endTime = System.nanoTime();

        long durationMs = (endTime - startTime) / 1_000_000;

        // Увеличиваем лимит до 1000ms (1 секунда) для тестовой среды
        assertTrue(durationMs < 1000, "Should process 1000 entries in < 1000ms, took: " + durationMs + "ms");
        assertTrue(balance >= 0);

        // Опционально: выводим время для мониторинга
        System.out.println("Processed 1000 entries in " + durationMs + "ms");
    }
}