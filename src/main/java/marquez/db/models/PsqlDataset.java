package marquez.db.models;

import lombok.AllArgsConstructor;
import org.postgresql.util.PGobject;

@AllArgsConstructor
public class PsqlDataset extends PGobject {
  public PsqlDataset() {
    setType("dataset");
  }
  public String name;

  public String getValue() {
    return "(" + name + ")";
  }
}
