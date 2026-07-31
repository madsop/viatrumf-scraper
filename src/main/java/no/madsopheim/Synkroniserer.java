package no.madsopheim;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import no.madsopheim.lagring.Lagring;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

@Dependent
class Synkroniserer {

    @Inject
    Lagring lagring;

    @RestClient
    SASOnlineShoppingClient sasOnlineShoppingClient;

    private static final String tidssone = "Europe/Oslo";

    @PostConstruct
    void init() {
        formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
        sasOnlineShoppingFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy");
        IO.println("Bruker lagring " + lagring.getClass().getSimpleName());
    }

    private DateTimeFormatter formatter;
    private DateTimeFormatter sasOnlineShoppingFormatter;

    void synkroniser() throws IOException, ExecutionException, InterruptedException {
        IO.println("Starter å synkronisere SAS Online Shopping");
        synkroniserSASOnlineShopping();
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

    private void synkroniserSASOnlineShopping() throws ExecutionException, InterruptedException {
        var no = ZonedDateTime.now(ZoneId.of(tidssone));
        String collectionNamn = "sasOnlineShopping";
        SASOnlineShoppingResponse response = sasOnlineShoppingClient.getSASOnlineShopping();
        IO.println("Fikk " + response.data().size() + " innslag i SAS Online Shopping");
        var futures = response.data().stream()
                .map(s -> konverterSASOnlineShop(s, no))
                .map(s -> lagring.lagre(s, collectionNamn));
        ApiFutures.allAsList(futures.filter(Objects::nonNull).collect(Collectors.toList())).get();
    }

    private SASOnlineShop konverterSASOnlineShop(Shop shop, ZonedDateTime no) {
        try {
            return new SASOnlineShop(
                    shop.name(),
                    "https://onlineshopping.flysas.com/nb-NO/butikker/" + shop.name().replace(" ", "-") + "/" + shop.uuid(),
                    formaterCommissionType(shop.commissionType()),
                    formaterCurrency(shop.currency()),
                    shop.points(),
                    Optional.ofNullable(shop.campaign_ends_date()).map(d -> LocalDate.parse(d, sasOnlineShoppingFormatter)).orElse(null),
                    shop.points_campaign(),
                    shop.points(),
                    no.format(formatter)
            );
        } catch (Exception e) {
            IO.println("Feila under synkronisering av " + shop.name());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private CommissionType formaterCommissionType(String commissionType) {
        if (commissionType.equals("fixed")) {
            return CommissionType.fixed;
        }
        if (commissionType.equals("variable")) {
            return CommissionType.variable;
        }
        throw new IllegalArgumentException("Forventa ikke å få commission type som ikkje var fixed eller variable, var " + commissionType);
    }

    private Currency formaterCurrency(String currency) {
        if (currency.equals("%")) {
            return Currency.PROSENT;
        }
        if (currency.equals("NOK")) {
            return Currency.NOK;
        }
        throw new IllegalArgumentException("Forventa ikke å få valuta som ikkje var prosent eller NOK, var " + currency);
    }
}

record SynkroniseringInternalRequest(
    Element element,
    ZonedDateTime no,
    DateTimeFormatter formatter
) {}

record SASOnlineShop(
        String namn,
        String href,
        CommissionType commissionType,
        Currency currency,
        Double points,
        LocalDate campaignEndsDate,
        Double pointsCampaign,
        Double pointsChannel,
        String timestamp
) implements Innslag {}

enum CommissionType {
    fixed,
    variable
}

enum Currency {
    PROSENT,
    NOK
}