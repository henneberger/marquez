package marquez.db.model;

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
