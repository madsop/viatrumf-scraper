package no.madsopheim;

public record TrumfNetthandelInnslag(
        String namn,
        String verdi,
        String href,
        String popularitet,
        String timestamp
) implements Innslag {
}
