package no.madsopheim;

public record Innslag(
        String namn,
        String verdi,
        String href,
        String popularitet,
        String timestamp
) {
}
