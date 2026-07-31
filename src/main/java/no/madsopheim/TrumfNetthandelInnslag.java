package no.madsopheim;

import org.jsoup.nodes.Element;

public record TrumfNetthandelInnslag(
        String namn,
        String verdi,
        String href,
        String popularitet,
        String timestamp
) implements Innslag {
    public static TrumfNetthandelInnslag create(SynkroniseringInternalRequest request) {
        Element butikk = request.element();
        String namn = butikk.attribute("data-name").getValue();
        String verdi = butikk.attribute("data-percentage").getValue();
        String popularitet = butikk.attribute("data-popularity").getValue();
        String href = butikk.attribute("href").getValue();
        String timestamp = request.no().format(request.formatter());
        return new TrumfNetthandelInnslag(namn, verdi, href, popularitet, timestamp);
    }
}
