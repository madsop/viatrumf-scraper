package no.madsopheim;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.io.IOException;

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
        } catch (IOException e) {
            IO.println("Feila under synkronisering");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
