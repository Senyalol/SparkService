package test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import spark.TransactionEntry;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static spark.AnomalyDetectionFunction.purgeWindow;

@DisplayName("Purge Window Tests")
class PurgeWindowTest {

    private List<TransactionEntry> entries;
    private static final long WINDOW_MS = 300000; // 5 minutes

    @BeforeEach
    void setUp() {
        entries = new ArrayList<>();
    }

    @Test
    @DisplayName("Should remove entries older than window")
    void shouldRemoveEntriesOlderThanWindow() {
        long now = System.currentTimeMillis();
        entries.add(new TransactionEntry(now - 600000, 100.0, "Deposit")); // 10 min ago
        entries.add(new TransactionEntry(now - 120000, 200.0, "Deposit")); // 2 min ago
        entries.add(new TransactionEntry(now - 30000, 300.0, "Deposit"));  // 0.5 min ago

        purgeWindow(entries, now, WINDOW_MS);

        assertEquals(2, entries.size(), "Should have 2 entries after purge");

        // Проверяем, что все оставшиеся записи не старше окна
        for (TransactionEntry entry : entries) {
            assertTrue(entry.getTimestamp() > now - WINDOW_MS,
                    "Entry timestamp should be within window: " + entry.getTimestamp());
        }

        // Проверяем, что удалилась старая запись
        boolean hasOldEntry = entries.stream()
                .anyMatch(e -> e.getTimestamp() == now - 600000);
        assertFalse(hasOldEntry, "Old entry (10 min) should be removed");
    }

    @Test
    @DisplayName("Should keep entries within window")
    void shouldKeepEntriesWithinWindow() {
        long now = System.currentTimeMillis();
        entries.add(new TransactionEntry(now - 60000, 100.0, "Deposit"));   // 1 min ago
        entries.add(new TransactionEntry(now - 120000, 200.0, "Deposit"));  // 2 min ago

        purgeWindow(entries, now, WINDOW_MS);

        assertEquals(2, entries.size(), "Both entries should be kept");

        // Проверяем, что обе записи остались
        assertTrue(entries.stream().anyMatch(e -> e.getTimestamp() == now - 60000),
                "1 min entry should be kept");
        assertTrue(entries.stream().anyMatch(e -> e.getTimestamp() == now - 120000),
                "2 min entry should be kept");
    }

    @Test
    @DisplayName("Should handle empty list")
    void shouldHandleEmptyList() {
        purgeWindow(entries, System.currentTimeMillis(), WINDOW_MS);
        assertTrue(entries.isEmpty(), "Empty list should remain empty");
        assertEquals(0, entries.size(), "Size should be 0");
    }

    @Test
    @DisplayName("Should handle boundary (timestamp exactly at window edge)")
    void shouldHandleBoundary() {
        long now = System.currentTimeMillis();
        entries.add(new TransactionEntry(now - WINDOW_MS, 100.0, "Deposit"));

        purgeWindow(entries, now, WINDOW_MS);

        assertEquals(1, entries.size(), "Entry at exact window edge should be kept");

        // Проверяем, что это та же самая запись
        assertEquals(now - WINDOW_MS, entries.get(0).getTimestamp(),
                "Timestamp should match the original");
    }

    @Test
    @DisplayName("Should remove all entries when all are older")
    void shouldRemoveAllWhenAllAreOlder() {
        long now = System.currentTimeMillis();
        entries.add(new TransactionEntry(now - 600000, 100.0, "Deposit"));
        entries.add(new TransactionEntry(now - 700000, 200.0, "Deposit"));
        entries.add(new TransactionEntry(now - 800000, 300.0, "Deposit"));

        purgeWindow(entries, now, WINDOW_MS);

        assertTrue(entries.isEmpty(), "All entries should be removed");
        assertEquals(0, entries.size());
    }

    @Test
    @DisplayName("Should keep all entries when all are within window")
    void shouldKeepAllWhenAllAreWithinWindow() {
        long now = System.currentTimeMillis();
        entries.add(new TransactionEntry(now - 60000, 100.0, "Deposit"));
        entries.add(new TransactionEntry(now - 120000, 200.0, "Deposit"));
        entries.add(new TransactionEntry(now - 180000, 300.0, "Deposit"));

        purgeWindow(entries, now, WINDOW_MS);

        assertEquals(3, entries.size(), "All entries should be kept");
    }

}