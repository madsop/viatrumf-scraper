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
class Synkroniserer {

    @Inject
    Lagring lagring;

    @Inject
    SASOnlineShoppingService sasOnlineShoppingService;

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

    void synkroniser() throws IOException, ExecutionException, InterruptedException {
        IO.println("Starter å synkronisere SAS Online Shopping");
        sasOnlineShoppingService.synkroniserSASOnlineShopping();
        IO.println("Ferdig med å synkronisere SAS Online Shopping");
        IO.println("Starter synkronisering av Trumf Netthandel");
        synkroniserTrumfNetthandel();
        IO.println("Ferdig med å synkronisere Trumf Netthandel");
    }

    private void synkroniserTrumfNetthandel() throws IOException, ExecutionException, InterruptedException {
        synkroniser("a.merchant-tile", "https://trumfnetthandel.no/category/paged/all/500", "viatrumf-scraper2", TrumfNetthandelInnslag::create);
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
}

record SynkroniseringInternalRequest(
    Element element,
    ZonedDateTime no,
    DateTimeFormatter formatter
) {}