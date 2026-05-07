namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public static class TestMongoConnectionStrings
{
    public static string Default => Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING") ??
                                    "mongodb://mongo1:27017,mongo2:27018,mongo3:27019/?replicaSet=rs0";
}