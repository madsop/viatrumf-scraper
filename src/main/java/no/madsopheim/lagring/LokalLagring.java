package no.madsopheim.lagring;

import com.google.api.core.ApiFuture;
import io.quarkus.arc.profile.IfBuildProfile;
import jakarta.enterprise.context.Dependent;
import no.madsopheim.TrumfNetthandelInnslag;

@Dependent
@IfBuildProfile("dev")
public class LokalLagring implements Lagring {
    @Override
    public ApiFuture<?> lagre(TrumfNetthandelInnslag innslag, String collectionNamn) {
        return null;
    }
}
