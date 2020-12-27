package marquez.api;

import lombok.experimental.Delegate;
import marquez.service.models.Namespace;

public class NamespaceResponse {
  @Delegate
  private final Namespace namespace;

  public NamespaceResponse(Namespace namespace) {
    this.namespace = namespace;
  }

  public String getOwnerName() {
    if (namespace.getCurrentOwner() != null) {
      return namespace.getCurrentOwner().getName();
    }
    return null;
  }
}
