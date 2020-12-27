package marquez.api;

import lombok.experimental.Delegate;
import marquez.service.models.Namespace;

public class NamespaceContract {
  @Delegate
  private final Namespace namespace;

  public NamespaceContract(Namespace namespace) {
    this.namespace = namespace;
  }

  public String getOwnerName() {
    if (namespace.getCurrentOwner() != null) {
      return namespace.getCurrentOwner().getName();
    }
    return null;
  }
}
