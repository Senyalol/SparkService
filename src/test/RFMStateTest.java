package test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import spark.SparkStreamingApp;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RFMState Unit Tests")
class RFMStateTest {

    private SparkStreamingApp.RFMState state;

    @BeforeEach
    void setUp() {
        state = new SparkStreamingApp.RFMState();
    }

    @Nested
    @DisplayName("Initial state tests")
    class InitialStateTests {

        @Test
        @DisplayName("Should initialize with default values")
        void shouldInitializeWithDefaultValues() {
            assertEquals(0, state.getLastTs(), "lastTs should be 0");
            assertEquals(0, state.getFirstTs(), "firstTs should be 0");
            assertEquals(0, state.getLastWallMs(), "lastWallMs should be 0");
            assertEquals(0.0, state.getMTotal(), "mTotal should be 0");
            assertEquals(0.0, state.getMWindow(), "mWindow should be 0");
            assertEquals(0, state.getFTotal(), "fTotal should be 0");
            assertEquals(0, state.getFWindow(), "fWindow should be 0");
            assertEquals(0.0, state.getRMinutes(), "rMinutes should be 0");
            assertTrue(state.getEntries().isEmpty(), "Entries should be empty");
        }
    }

    @Nested
    @DisplayName("Setters and getters tests")
    class SettersGettersTests {

        @Test
        @DisplayName("Should set and get lastTs correctly")
        void shouldSetAndGetLastTs() {
            long expected = 1234567890L;
            state.setLastTs(expected);
            assertEquals(expected, state.getLastTs(), "lastTs should match");
        }

        @Test
        @DisplayName("Should set and get firstTs correctly")
        void shouldSetAndGetFirstTs() {
            long expected = 1234567890L;
            state.setFirstTs(expected);
            assertEquals(expected, state.getFirstTs(), "firstTs should match");
        }

        @Test
        @DisplayName("Should set and get mTotal correctly")
        void shouldSetAndGetMTotal() {
            double expected = 5000.50;
            state.setMTotal(expected);
            assertEquals(expected, state.getMTotal(), 0.001, "mTotal should match");
        }

        @Test
        @DisplayName("Should set and get mWindow correctly")
        void shouldSetAndGetMWindow() {
            double expected = 1500.25;
            state.setMWindow(expected);
            assertEquals(expected, state.getMWindow(), 0.001, "mWindow should match");
        }

        @Test
        @DisplayName("Should set and get fTotal correctly")
        void shouldSetAndGetFTotal() {
            long expected = 42L;
            state.setFTotal(expected);
            assertEquals(expected, state.getFTotal(), "fTotal should match");
        }

        @Test
        @DisplayName("Should set and get fWindow correctly")
        void shouldSetAndGetFWindow() {
            long expected = 10L;
            state.setFWindow(expected);
            assertEquals(expected, state.getFWindow(), "fWindow should match");
        }

        @Test
        @DisplayName("Should set and get rMinutes correctly")
        void shouldSetAndGetRMinutes() {
            double expected = 15.5;
            state.setRMinutes(expected);
            assertEquals(expected, state.getRMinutes(), 0.001, "rMinutes should match");
        }
    }

    @Nested
    @DisplayName("Backward compatibility tests")
    class BackwardCompatibilityTests {

        @Test
        @DisplayName("getF() should return fWindow value")
        void getFShouldReturnFWindow() {
            state.setFWindow(25L);
            assertEquals(25L, state.getF(), "getF() should return fWindow");
        }

        @Test
        @DisplayName("getM() should return mWindow value")
        void getMShouldReturnMWindow() {
            state.setMWindow(1234.56);
            assertEquals(1234.56, state.getM(), 0.001, "getM() should return mWindow");
        }

        @Test
        @DisplayName("setF() should update fWindow")
        void setFShouldUpdateFWindow() {
            state.setF(30L);
            assertEquals(30L, state.getFWindow(), "setF() should update fWindow");
        }

        @Test
        @DisplayName("setM() should update mWindow")
        void setMShouldUpdateMWindow() {
            state.setM(3000.0);
            assertEquals(3000.0, state.getMWindow(), 0.001, "setM() should update mWindow");
        }
    }
}