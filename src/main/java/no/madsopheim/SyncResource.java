package no.madsopheim;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.bouncycastle.cert.ocsp.Req;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@ApplicationScoped
public class SyncResource {

    @Inject
    Synkroniserer synkroniserer;

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