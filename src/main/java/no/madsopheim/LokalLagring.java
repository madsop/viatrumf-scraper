package no.madsopheim;

import io.quarkus.arc.profile.IfBuildProfile;
import jakarta.enterprise.context.Dependent;

@Dependent
@IfBuildProfile("dev")
public class LokalLagring implements Lagring {
    @Override
    public void lagre(Innslag innslag) {

    }
}
