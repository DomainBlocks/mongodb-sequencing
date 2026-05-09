namespace DomainBlocks.MongoDB.Sequencing.Tests.Integration;

public static class TestMongoConnectionStrings
{
    public static string Default => "mongodb://mongo1:27017,mongo2:27018,mongo3:27019/?replicaSet=rs0";
}