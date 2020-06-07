# Drone Simulator & CSVtoStream

The Go code implements a 
- drone simulation regularly sending messages to an Azure EventHubs (considered as an Apache Kafka stream)
- csv sender, that sends each line of the given .csv file as a message to the same EventHubs

# Build
To build the go app, follow these steps
- install Go and setup the Go environment, such as assigning the different values (GOPATH, etc)
- clone the repo following this path: `$GOPATH/src/github.com/nicofeals/prestacop`
- export `KAFKA_EVENTHUB_CONNECTION_STRING` with the correct value for the Azure EventHubs
```
cd cmd/
go build .
```
This will produce the `cmd` binary

# Run
If you want a global overview of how to run the app, simply run `./cmd -h`.
- To see how to launch the drone simulator, run `./cmd drone -h`. 
This will display a small help explaining how to use it, and what the arguments are.
- Accordingly, to run the CSVtoStream, run `./cmd csv -h`. 
Again, this will display a help explaining how to launch it properly

__NOTE__: Running `./cmd drone` or `./cmd csv` as is will surely fail, as some arguments are mandatory, and their default value (if they have one) won't be the appropriate one.
