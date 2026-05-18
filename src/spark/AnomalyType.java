package spark;

public enum AnomalyType {
    BIGGER_THEN_AVG_CHECK, // Credit/Deposit >= 3 * avg за 5 мин → alerts + user-segments (если Credit <= M)
    NEGATIVE_M, // Credit > M → только alerts
    BIGGEST_AND_FREQUENT_CREDIT // Частые крупные Credit/Deposit за 5 мин → alerts + user-segments (если Credit <= M)
}