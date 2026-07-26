package no.madsopheim;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Path("/synkroniser")
public class SyncResource {

    @Inject
    Synkroniserer synkroniserer;

    @GET
    public String synkroniserSomGet() {
        return synkroniser(new Object());
    }

    @POST
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.APPLICATION_JSON)
    public String synkroniser(Object request) {
        IO.println("Starter sync");
        try {
            synkroniserer.synkroniser();
            IO.println("Ferdig med synk");
            return "Ferdig med synk";
        } catch (IOException | ExecutionException | InterruptedException e) {
            IO.println("Feila under synkronisering");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
