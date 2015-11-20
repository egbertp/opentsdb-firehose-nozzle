# Summary
The opentsdb-firehose-nozzle is a CF component which forwards metrics from the Loggregator Firehose to an [Opentsdb] deployment

# Configure CloudFoundry UAA for Firehose Nozzle

The opentsdb firehose nozzle requires a UAA user who is authorized to access the loggregator firehose. You can add a user by editing your CloudFoundry manifest to include the details about this user under the properties.uaa.clients section. For example to add a user `opentsdb-firehose-nozzle`:

```
properties:
  uaa:
    clients:
      opentsdb-firehose-nozzle:
        access-token-validity: 1209600
        authorized-grant-types: authorization_code,client_credentials,refresh_token
        override: true
        secret: <password>
        scope: openid,oauth.approvals,doppler.firehose
        authorities: oauth.login,doppler.firehose
```

# Running

The opentsdb nozzle uses a configuration file to obtain the firehose URL, and other configuration parameters. The firehose and the opentsdb servers both require authentication -- the firehose requires a valid username/password and opentsdb requires a valid API key.

You can start the firehose nozzle by executing:
```
go run main.go -config config/opentsdb-firehose-nozzle.json
```

# Batching

The configuration file specifies the interval at which the nozzle will flush metrics to opentsdb. By default this is set to 15 seconds.

# Tests

You need [ginkgo](http://onsi.github.io/ginkgo/) and go 1.5+ to run the tests. The tests can be executed by:
```
go build
ginkgo -r

```
