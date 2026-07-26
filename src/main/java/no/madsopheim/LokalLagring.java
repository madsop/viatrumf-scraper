package no.madsopheim;

import com.google.api.core.ApiFuture;
import io.quarkus.arc.profile.IfBuildProfile;
import jakarta.enterprise.context.Dependent;

@Dependent
@IfBuildProfile("dev")
public class LokalLagring implements Lagring {
    @Override
    public ApiFuture<?> lagre(Innslag innslag) {
        return null;
    }
}
