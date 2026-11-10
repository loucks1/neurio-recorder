# Synthetic RatePlanTool fixtures

These files are synthetic. They record RatePlanTool commit
`78fc57c348e93e11cc6b3cf26f02fb292c54bbfe`.

`rateplantool-synthetic-samples.json` is a `GET samples` body.
SHA-256 `312a2bd55403487ecdebf494995a3ac645a5262649b330ec02fe0625f3da9509`.
It is not a reading from a LAN device and it is not a collector export.

`rateplantool-stub-contract.json` records a synthetic token response, a
synthetic `users/current` body, and the samples query. The bearer string
inside it is a placeholder. It is not a credential and this collector does
not accept it.

Passing tests against these files does not mean that reader is wired to this
collector. `rateplantool_contract` remains false.
