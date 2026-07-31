package no.madsopheim.lagring;

import com.google.api.core.ApiFuture;
import no.madsopheim.TrumfNetthandelInnslag;

public interface Lagring {
    ApiFuture<?> lagre(TrumfNetthandelInnslag innslag, String collectionNamn);
}
