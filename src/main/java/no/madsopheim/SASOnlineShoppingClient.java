package no.madsopheim;

import jakarta.enterprise.context.Dependent;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import java.util.List;
import java.util.UUID;

@RegisterRestClient
@Dependent
interface SASOnlineShoppingClient {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    SASOnlineShoppingResponse getSASOnlineShopping();
}

record SASOnlineShoppingResponse(List<Shop> data) {}

record Shop(
        UUID uuid,
        String name,
        String commission_type,
        String currency,
        Double points,
        String campaign_ends_date,
        Double points_campaign,
        Double points_channel
) {}