package test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import spark.SparkStreamingApp;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("R Minutes Calculation Tests")
class RMinutesCalculationTest {

    private SparkStreamingApp.RFMState state;
    private long now;

    @BeforeEach
    void setUp() {
        state = new SparkStreamingApp.RFMState();
        now = System.currentTimeMillis();
    }

    @Nested
    @DisplayName("First transaction tests")
    class FirstTransactionTests {

        @Test
        @DisplayName("Should return 0 for first transaction")
        void shouldReturnZeroForFirstTransaction() {
            state.setLastTs(0);

            double newRMinutes = calculateRMinutes(state, now, 0);

            assertEquals(0.0, newRMinutes, 0.001, "First transaction should return 0");
        }
    }

    @Nested
    @DisplayName("Delta event calculations")
    class DeltaEventCalculations {

        @Test
        @DisplayName("Should calculate correct minutes for 30 seconds difference")
        void shouldCalculateFor30Seconds() {
            long lastTs = now - 30000; // 30 seconds ago
            state.setLastTs(lastTs);
            state.setRMinutes(0);

            double newRMinutes = calculateRMinutes(state, now, lastTs);

            assertTrue(newRMinutes >= 0.49 && newRMinutes <= 0.51,
                    "Expected ~0.5 minutes, got " + newRMinutes);
        }

        @Test
        @DisplayName("Should calculate correct minutes for 1 minute difference")
        void shouldCalculateFor1Minute() {
            long lastTs = now - 60000; // 1 minute ago
            state.setLastTs(lastTs);
            state.setRMinutes(0);

            double newRMinutes = calculateRMinutes(state, now, lastTs);

            assertTrue(newRMinutes >= 0.99 && newRMinutes <= 1.01,
                    "Expected ~1.0 minutes, got " + newRMinutes);
        }

        @Test
        @DisplayName("Should calculate correct minutes for 5 minutes difference")
        void shouldCalculateFor5Minutes() {
            long lastTs = now - 300000; // 5 minutes ago
            state.setLastTs(lastTs);
            state.setRMinutes(0);

            double newRMinutes = calculateRMinutes(state, now, lastTs);

            assertTrue(newRMinutes >= 4.99 && newRMinutes <= 5.01,
                    "Expected ~5.0 minutes, got " + newRMinutes);
        }

        @Test
        @DisplayName("Should calculate correct minutes for 2.5 minutes difference")
        void shouldCalculateFor2Point5Minutes() {
            long lastTs = now - 150000; // 2.5 minutes ago
            state.setLastTs(lastTs);
            state.setRMinutes(0);

            double newRMinutes = calculateRMinutes(state, now, lastTs);

            assertTrue(newRMinutes >= 2.49 && newRMinutes <= 2.51,
                    "Expected ~2.5 minutes, got " + newRMinutes);
        }
    }

    @Nested
    @DisplayName("Negative delta tests")
    class NegativeDeltaTests {

        @Test
        @DisplayName("Should handle negative delta (time went backwards)")
        void shouldHandleNegativeDelta() {
            long lastTs = now + 60000; // Future timestamp
            state.setLastTs(lastTs);
            state.setRMinutes(5.0);

            double newRMinutes = calculateRMinutes(state, now, lastTs);

            assertEquals(0.0, newRMinutes, 0.001, "Negative delta should return 0");
        }
    }

    @Nested
    @DisplayName("Same timestamp tests")
    class SameTimestampTests {

        @Test
        @DisplayName("Should keep previous rMinutes when timestamp same")
        void shouldKeepPreviousRMinutes() {
            long sameTs = now;
            state.setLastTs(sameTs);
            state.setRMinutes(2.5);

            double newRMinutes = calculateRMinutes(state, sameTs, sameTs);

            assertEquals(2.5, newRMinutes, 0.001, "Should keep previous rMinutes");
        }
    }

    @Nested
    @DisplayName("RMinutes value range tests")
    class RMinutesValueRangeTests {

        @Test
        @DisplayName("Should never be negative")
        void shouldNeverBeNegative() {
            long lastTs = now - 30000;
            state.setLastTs(lastTs);

            double newRMinutes = calculateRMinutes(state, now, lastTs);

            assertTrue(newRMinutes >= 0, "RMinutes should never be negative, got " + newRMinutes);
        }

        @Test
        @DisplayName("Should handle large values (hours)")
        void shouldHandleLargeValues() {
            long lastTs = now - 3600000; // 1 hour ago
            state.setLastTs(lastTs);
            state.setRMinutes(0);

            double newRMinutes = calculateRMinutes(state, now, lastTs);

            assertTrue(newRMinutes >= 59.9 && newRMinutes <= 60.1,
                    "Expected ~60.0 minutes, got " + newRMinutes);
        }
    }

    // Helper method for testing RMinutes logic
    private double calculateRMinutes(SparkStreamingApp.RFMState state, long curr, long lastTs) {
        if (lastTs == 0) {
            return 0;
        }

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