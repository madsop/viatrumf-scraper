package no.madsopheim;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Path("/synkroniser")
public class SyncResource {

    @Inject
    Synkroniserer synkroniserer;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public void synkroniser() {
        IO.println("Starter sync");
        try {
            synkroniserer.synkroniser();
            IO.println("Ferdig med synk");
        } catch (IOException | ExecutionException | InterruptedException e) {
            IO.println("Feila under synkronisering");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
