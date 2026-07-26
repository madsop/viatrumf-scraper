package no.madsopheim;

import com.google.api.core.ApiFuture;

public interface Lagring {
    ApiFuture<?> lagre(Innslag innslag);
}
