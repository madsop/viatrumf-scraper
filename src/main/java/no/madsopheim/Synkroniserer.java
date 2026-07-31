package no.madsopheim;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import no.madsopheim.lagring.Lagring;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Dependent
class Synkroniserer {

    @Inject
    Lagring lagring;

    private static final String tidssone = "Europe/Oslo";

    @PostConstruct
    void init() {
        formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
        IO.println("Bruker lagring " + lagring.getClass().getSimpleName());
    }

    private DateTimeFormatter formatter;

    void synkroniser() throws IOException, ExecutionException, InterruptedException {
        IO.println("Starter synkronisering av Trumf Netthandel");
        synkroniserTrumfNetthandel();
        IO.println("Ferdig med å synkronisere Trumf Netthandel");
    }

    private void synkroniserTrumfNetthandel() throws IOException, ExecutionException, InterruptedException {
        synkroniser("a.merchant-tile", "https://trumfnetthandel.no/category/paged/all/500", "viatrumf-scraper2");
    }

    private void synkroniser(String selectFilter, String url, String collectionNamn) throws IOException, ExecutionException, InterruptedException {
        var no = ZonedDateTime.now(ZoneId.of(tidssone));
        var futures = new ArrayList<ApiFuture<?>>();
        for (Element butikk : Jsoup.connect(url).get().select(selectFilter)) {
            Innslag innslag = formaterTrumfNetthandelInnslag(butikk, no);
            futures.add(lagring.lagre(innslag, collectionNamn));
        }
        IO.println("Håndterer " + futures.size() + " innslag");
        ApiFutures.allAsList(futures.stream().filter(Objects::nonNull).collect(Collectors.toList())).get();
    }

    private Innslag formaterTrumfNetthandelInnslag(Element butikk, ZonedDateTime no) {
        String namn = butikk.attribute("data-name").getValue();
        String verdi = butikk.attribute("data-percentage").getValue();
        String popularitet = butikk.attribute("data-popularity").getValue();
        String href = butikk.attribute("href").getValue();
        String timestamp = no.format(formatter);
        return new TrumfNetthandelInnslag(namn, verdi, href, popularitet, timestamp);
    }
}
