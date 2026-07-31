package no.madsopheim;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

@Dependent
class Synkroniserer {

    @Inject
    SASOnlineShoppingService sasOnlineShoppingService;

    @Inject
    TrumfNetthandelService trumfNetthandelService;

    void synkroniser() throws IOException, ExecutionException, InterruptedException {
        IO.println("Starter å synkronisere SAS Online Shopping");
        sasOnlineShoppingService.synkroniserSASOnlineShopping();
        IO.println("Ferdig med å synkronisere SAS Online Shopping");
        IO.println("Starter synkronisering av Trumf Netthandel");
        trumfNetthandelService.synkroniserTrumfNetthandel();
        IO.println("Ferdig med å synkronisere Trumf Netthandel");
    }
}