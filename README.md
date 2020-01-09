# Restarter

The `restarter` script is intended to be used for restarting kubernetes deployments that act as Kinesis
consumers. It assumes that there is some environment variable with a name like `LEASE_TABLE` (although that is configurable).

## Building the binary

From the repository's folder

```sh
go build -o restarter
```

## Example Usage

```sh
./restarter -d ace-decider-us-east-2-staging -c decider --region us-east-2
```
