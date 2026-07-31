package no.madsopheim;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import no.madsopheim.lagring.Lagring;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

@Dependent
public class TrumfNetthandelService {

    @Inject
    Lagring lagring;

    @ConfigProperty(name = "tidssone")
    String tidssone;

    @ConfigProperty(name = "tidsformat")
    String tidsformat;

    @PostConstruct
    void init() {
        formatter = DateTimeFormatter.ofPattern(tidsformat);
        IO.println("Bruker lagring " + lagring.getClass().getSimpleName());
    }

    private DateTimeFormatter formatter;

    public void synkroniserTrumfNetthandel() throws IOException, ExecutionException, InterruptedException {
        synkroniser("a.merchant-tile", "https://trumfnetthandel.no/category/paged/all/500", "viatrumf-scraper2", this::create);
    }

    private <T extends Innslag> void synkroniser(String selectFilter, String url, String collectionNamn, Function<SynkroniseringInternalRequest, T> function) throws IOException, ExecutionException, InterruptedException {
        var no = ZonedDateTime.now(ZoneId.of(tidssone));
        var futures = new ArrayList<ApiFuture<?>>();
        for (Element butikk : Jsoup.connect(url).get().select(selectFilter)) {
            Innslag innslag = function.apply(new SynkroniseringInternalRequest(butikk, no, formatter));
            futures.add(lagring.lagre(innslag, collectionNamn));
        }
        IO.println("Håndterer " + futures.size() + " innslag");
        ApiFutures.allAsList(futures.stream().filter(Objects::nonNull).collect(Collectors.toList())).get();
    }

    private TrumfNetthandelInnslag create(SynkroniseringInternalRequest request) {
        Element butikk = request.element();
        String namn = butikk.attribute("data-name").getValue();
        String verdi = butikk.attribute("data-percentage").getValue();
        String popularitet = butikk.attribute("data-popularity").getValue();
        String href = butikk.attribute("href").getValue();
        String timestamp = request.no().format(request.formatter());
        return new TrumfNetthandelInnslag(namn, verdi, href, popularitet, timestamp);
    }
}

record SynkroniseringInternalRequest(
        Element element,
        ZonedDateTime no,
        DateTimeFormatter formatter
) {}