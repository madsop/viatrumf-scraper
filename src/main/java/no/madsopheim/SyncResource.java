package no.madsopheim;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@ApplicationScoped
@Path("/")
public class SyncResource {

    @Inject
    SASOnlineShoppingService sasOnlineShoppingService;

    @Inject
    TrumfNetthandelService trumfNetthandelService;

    @GET
    @Path("/synkroniserGet")
    public String synkroniserSomGet() {
        return synkroniser(new Request("get-kallet"));
    }

    @POST
    @Path("/synkroniser")
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public String synkroniser(Request request) {
        IO.println("Starter sync med request " + request.source());
        try {
            IO.println("Starter å synkronisere SAS Online Shopping");
            sasOnlineShoppingService.synkroniserSASOnlineShopping();
            IO.println("Ferdig med å synkronisere SAS Online Shopping");
            IO.println("Starter synkronisering av Trumf Netthandel");
            trumfNetthandelService.synkroniserTrumfNetthandel();
            IO.println("Ferdig med å synkronisere Trumf Netthandel");
            IO.println("Ferdig med synk");
            return "Ferdig med synk";
        } catch (IOException | ExecutionException | InterruptedException e) {
            IO.println("Feila under synkronisering");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}