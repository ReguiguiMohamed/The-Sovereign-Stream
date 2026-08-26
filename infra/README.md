# Infrastructure

No Terraform root exists yet. Infrastructure begins only after the local Flink
and Bigtable-emulator gate passes and the target region, cost limit and shutdown
procedure are reviewed. `apply` and `destroy` always require explicit approval.
