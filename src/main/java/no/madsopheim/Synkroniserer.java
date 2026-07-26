package no.madsopheim;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

@Dependent
class Synkroniserer {

    @Inject
    Lagring lagring;

    private static final String selectFilter = "a.merchant-tile";
    private static final String tidssone = "Europe/Oslo";
    private static final String url = "https://trumfnetthandel.no/category/paged/all/500";

    @PostConstruct
    void init() {
        formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
        IO.println("Bruker lagring " + lagring.getClass().getSimpleName());
    }

    private DateTimeFormatter formatter;

    void synkroniser() throws IOException {
        var no = ZonedDateTime.now(ZoneId.of(tidssone));
        for (Element butikk : Jsoup.connect(url).get().select(selectFilter)) {
            Innslag innslag = formaterInnslag(butikk, no);
            lagring.lagre(innslag);
        }
    }

    private Innslag formaterInnslag(Element butikk, ZonedDateTime no) {
        String namn = butikk.attribute("data-name").getValue();
        String verdi = butikk.attribute("data-percentage").getValue();
        String popularitet = butikk.attribute("data-popularity").getValue();
        String href = butikk.attribute("href").getValue();
        String timestamp = no.format(formatter);
        return new Innslag(namn, verdi, href, popularitet, timestamp);
    }
}
