package no.madsopheim.lagring;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteResult;
import io.quarkus.arc.profile.UnlessBuildProfile;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import no.madsopheim.Innslag;

@Dependent
@UnlessBuildProfile("dev")
public class FirestoreLagring implements Lagring {

    @Inject
    Firestore firestore;

    @Override
    public ApiFuture<WriteResult> lagre(Innslag innslag, String collectionNamn) {
        String escapedNamn = innslag.namn().replace(" ", "_").replace("'", "");
        CollectionReference collection = firestore.collection(collectionNamn);
        IO.println("Lagrer " + escapedNamn);
        DocumentReference document = collection.document(escapedNamn).collection("innslag").document(escapedNamn + "_" + innslag.timestamp() + ".json");
        return document.set(innslag);
    }
}
