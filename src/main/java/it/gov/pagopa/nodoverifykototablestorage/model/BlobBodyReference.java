package it.gov.pagopa.nodoverifykototablestorage.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BlobBodyReference {
  private String storageAccount;
  private String containerName;
  private String fileName;
  private long fileLength;

  @Override
  public String toString() {
    return String.format("{\"storageAccount\":\"%s\",\"containerName\":\"%s\",\"fileName\":\"%s\",\"fileLength\":%d}",
            this.storageAccount, this.containerName, this.fileName, this.fileLength);
  }
}
