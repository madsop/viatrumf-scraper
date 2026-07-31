package no.madsopheim.trumfnetthandel;

import no.madsopheim.Innslag;

public record TrumfNetthandelInnslag(
        String namn,
        String verdi,
        String href,
        String popularitet,
        String timestamp
) implements Innslag { }
