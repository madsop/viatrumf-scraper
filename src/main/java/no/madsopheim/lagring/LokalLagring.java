package no.madsopheim.lagring;

import com.google.api.core.ApiFuture;
import io.quarkus.arc.profile.IfBuildProfile;
import jakarta.enterprise.context.Dependent;
import no.madsopheim.Innslag;

@Dependent
@IfBuildProfile("dev")
public class LokalLagring implements Lagring {
    @Override
    public ApiFuture<?> lagre(Innslag innslag, String collectionNamn) {
        return null;
    }
}
