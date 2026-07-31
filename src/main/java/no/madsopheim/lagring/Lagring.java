package no.madsopheim.lagring;

import com.google.api.core.ApiFuture;
import no.madsopheim.Innslag;

public interface Lagring {
    ApiFuture<?> lagre(Innslag innslag, String collectionNamn);
}
