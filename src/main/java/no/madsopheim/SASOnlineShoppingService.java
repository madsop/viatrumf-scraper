package no.madsopheim;

import com.google.api.core.ApiFutures;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import no.madsopheim.lagring.Lagring;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Dependent
public class SASOnlineShoppingService {

    @Inject
    Lagring lagring;

    @RestClient
    SASOnlineShoppingClient sasOnlineShoppingClient;

    @ConfigProperty(name = "tidssone")
    String tidssone;

    @ConfigProperty(name = "tidsformat")
    String tidsformat;

    private DateTimeFormatter formatter;

    @PostConstruct
    void init() {
        formatter = DateTimeFormatter.ofPattern(tidsformat);
        IO.println("Bruker lagring " + lagring.getClass().getSimpleName());
    }

    public void synkroniserSASOnlineShopping() throws ExecutionException, InterruptedException {
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
                    formaterCommissionType(shop.commission_type()),
                    formaterCurrency(shop.currency()),
                    shop.points(),
                    shop.campaign_ends_date(),
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
        if ("fixed".equals(commissionType)) {
            return CommissionType.fixed;
        }
        if ("variable".equals(commissionType)) {
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


record SASOnlineShop(
        String namn,
        String href,
        CommissionType commissionType,
        Currency currency,
        Double points,
        String campaignEndsDate,
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