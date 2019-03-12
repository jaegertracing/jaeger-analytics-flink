package io.jaegertracing.tracequality.score;

import io.jaegertracing.tracequality.model.QualityScore;
import io.jaegertracing.tracequality.model.Span;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Emits a passing score if the version number of a client is higher than the ones specified and a failing score
 * otherwise.
 */
public class MinimumClientVersion implements SpanBasedQualityScore {
    public static final String MIN_VERSION_CHECK = "MinimumClientVersionCheck";
    private final Map<String, String> languageToMinVersion = Collections.unmodifiableMap(new HashMap<String, String>() {{
        put("go", "2.6.0");
        put("python", "3.8.0");
        put("node", "3.0.0");
        put("java", "0.0.17");
    }});

    @Override
    public void computeScore(Span span, QualityScoreAggregator out) {
        if (span.getClientVersion() == null || span.getClientVersion().isEmpty()) {
            incrementFail(span, out);
            return;
        }

        String[] languageVersion = span.getClientVersion().toLowerCase().split("-");
        if (languageVersion.length != 2
                || languageToMinVersion.get(languageVersion[0]) == null
                || !languageVersion[1].matches("[0-9]+(\\.[0-9]+)*")) {
            incrementFail(span, out);
            return;
        }

        if (isGreaterThanOrEqualTo(languageToMinVersion.get(languageVersion[0]), languageVersion[1])) {
            out.getPass(span.getServiceName(), MIN_VERSION_CHECK).ifPresent(QualityScore::increment);
        } else {
            incrementFail(span, out);
        }
    }

    private void incrementFail(Span span, QualityScoreAggregator out) {
        out.getFail(span.getServiceName(), MIN_VERSION_CHECK).ifPresent(QualityScore::increment);
    }


    /**
     * Compares two semantic version strings
     *
     * @param left  semantic version string
     * @param right semantic version string
     * @return true if right is greater than or equal to left
     * false for all other cases
     */
    boolean isGreaterThanOrEqualTo(String left, String right) {
        String[] leftParts = left.split("\\.");
        String[] rightParts = right.split("\\.");

        int length = Math.min(leftParts.length, rightParts.length);
        try {
            for (int i = 0; i < length; i++) {
                int leftPart = Integer.parseInt(leftParts[i]);
                int rightPart = Integer.parseInt(rightParts[i]);
                if (leftPart < rightPart) {
                    return true;
                }
                if (leftPart > rightPart) {
                    return false;
                }
            }
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}
